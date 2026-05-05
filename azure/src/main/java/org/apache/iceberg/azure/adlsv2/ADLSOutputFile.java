/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.azure.adlsv2;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.LeaseAction;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.models.PathInfo;
import com.azure.storage.file.datalake.options.DataLakeFileAppendOptions;
import com.azure.storage.file.datalake.options.DataLakeFileFlushOptions;
import com.azure.storage.file.datalake.options.FileParallelUploadOptions;
import com.azure.storage.file.datalake.specialized.DataLakeLeaseClientBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.FileChecksumOutputStream;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;

class ADLSOutputFile extends BaseADLSFile implements AtomicOutputFile {

  private Long length;
  private final DataLakeRequestConditions conditions;

  ADLSOutputFile(
      String location,
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      MetricsContext metrics) {
    this(location, fileClient, azureProperties, 0, null, metrics);
  }

  ADLSOutputFile(
      String location,
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      long length,
      DataLakeRequestConditions conditions,
      MetricsContext metrics) {
    super(location, fileClient, azureProperties, metrics);
    this.length = length;
    this.conditions = conditions;
  }

  /**
   * Create an output stream for the specified location if the target object does not exist in Azure
   * at the time of invocation.
   *
   * @return output stream
   */
  @Override
  public PositionOutputStream create() {
    if (!exists()) {
      return createOrOverwrite();
    } else {
      throw new AlreadyExistsException("Location already exists: %s", location());
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    try {
      return new ADLSOutputStream(fileClient(), azureProperties(), metrics());
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to create output stream for location: " + location(), e);
    }
  }

  @Override
  public InputFile toInputFile() {
    return new ADLSInputFile(
        location(), length, fileClient(), azureProperties(), metrics(), conditions);
  }

  @Override
  public CAS prepare(Supplier<InputStream> source, Strategy howto) {
    final ADLSChecksum checksum = new ADLSChecksum(howto);
    try (InputStream in = source.get();
        FileChecksumOutputStream chk =
            new FileChecksumOutputStream(ByteStreams.nullOutputStream(), checksum)) {
      ByteStreams.copy(in, chk);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return checksum;
  }

  @Override
  public ADLSInputFile writeAtomic(CAS checksum, Supplier<InputStream> source) throws IOException {
    ADLSChecksum token = (ADLSChecksum) checksum;
    switch (token.getStrategy()) {
      case CAS:
        return replaceDestObj(token, source);
      case APPEND:
        return appendDestObj(token, source);
      default:
        throw new UnsupportedOperationException("Unrecognized strategy: " + token.getStrategy());
    }
  }

  // Default lease duration covers the append+flush span; on flush failure the lease is explicitly
  // released so a stale-etag writer doesn't lock out other writers for the full duration.
  private static final int APPEND_LEASE_SECONDS = 15;

  /**
   * Append to the live object atomically. Concurrent writers are serialized by acquiring a blob
   * lease that spans the two-phase {@code appendWithResponse} + {@code flushWithResponse}: without
   * the lease, two writers can both upload uncommitted data at the same offset and the flush-etag
   * winner ends up committing a loser's bytes. The lease is acquired atomically with append (via
   * {@link LeaseAction#ACQUIRE}) and released atomically with flush (via {@link
   * LeaseAction#RELEASE}); flush failure releases the lease in the catch block to keep the lock-out
   * window short.
   */
  private ADLSInputFile appendDestObj(ADLSChecksum checksum, Supplier<InputStream> source) {
    final String leaseId = UUID.randomUUID().toString();
    boolean leaseHeld = false;
    try {
      final long appendLen = checksum.contentLength();
      DataLakeFileAppendOptions appendOpts =
          new DataLakeFileAppendOptions()
              .setContentHash(checksum.contentChecksumBytes())
              .setLeaseAction(LeaseAction.ACQUIRE)
              .setProposedLeaseId(leaseId)
              .setLeaseDuration(APPEND_LEASE_SECONDS);
      fileClient()
          .appendWithResponse(source.get(), length, appendLen, appendOpts, null, Context.NONE);
      leaseHeld = true;

      DataLakeRequestConditions flushConditions =
          new DataLakeRequestConditions().setLeaseId(leaseId);
      if (conditions != null) {
        if (conditions.getIfMatch() != null) {
          flushConditions.setIfMatch(conditions.getIfMatch());
        }
        if (conditions.getIfNoneMatch() != null) {
          flushConditions.setIfNoneMatch(conditions.getIfNoneMatch());
        }
      }
      final DataLakeFileFlushOptions flushOpts =
          new DataLakeFileFlushOptions()
              .setClose(true)
              .setRequestConditions(flushConditions)
              .setUncommittedDataRetained(false)
              .setLeaseAction(LeaseAction.RELEASE);
      // throws on failure
      final Response<PathInfo> resp =
          fileClient().flushWithResponse(length + appendLen, flushOpts, null, Context.NONE);
      // RELEASE on the flush options drops the lease atomically with a successful flush.
      leaseHeld = false;
      // update length to orig len + append (succeeded)
      this.length += appendLen;
      final PathInfo info = resp.getValue();
      return new ADLSInputFile(
          location(),
          length,
          fileClient(),
          azureProperties(),
          metrics(),
          new DataLakeRequestConditions().setIfMatch(info.getETag()));
    } catch (DataLakeStorageException e) {
      if (409 == e.getStatusCode() && e.getErrorCode() != null) {
        String code = e.getErrorCode();
        if ("LeaseAlreadyPresent".equals(code)
            || "LeaseIdMismatchWithLeaseOperation".equals(code)
            || "LeaseAlreadyBroken".equals(code)
            || "LeaseIsBreakingAndCannotBeAcquired".equals(code)) {
          throw new SupportsAtomicOperations.AppendException("Concurrent writer holds lease", e);
        }
      }
      if (412 == e.getStatusCode()) {
        // precondition failed
        throw new SupportsAtomicOperations.AppendException("Target modified", e);
      }
      if (400 == e.getStatusCode() && e.getErrorCode().equals("InvalidFlushPosition")) {
        throw new SupportsAtomicOperations.AppendException("Wrong length", e);
      }
      throw e;
    } finally {
      // Belt-and-suspenders: if append succeeded but flush failed, release the lease eagerly so
      // we don't lock other writers out for the full lease duration.
      if (leaseHeld) {
        try {
          new DataLakeLeaseClientBuilder()
              .fileClient(fileClient())
              .leaseId(leaseId)
              .buildClient()
              .releaseLease();
        } catch (DataLakeStorageException ignored) {
          // best effort: lease will expire on its own
        }
      }
    }
  }

  @SuppressWarnings("deprecation") // not clear how else to support atomic CAS
  private ADLSInputFile replaceDestObj(ADLSChecksum checksum, Supplier<InputStream> source)
      throws IOException {
    // Annoyingly, the checksum is not validated server-side, but stored as metadata. The
    // partial-write problem is less of an issue using an InputStream, so the length validation can
    // suffice
    try (InputStream src = source.get()) {
      final Response<PathInfo> resp =
          fileClient()
              .uploadWithResponse(
                  new FileParallelUploadOptions(src, checksum.contentLength())
                      .setRequestConditions(conditions)
                      .setHeaders(
                          new PathHttpHeaders()
                              .setContentMd5(checksum.contentChecksumBytes())
                              .setContentType("binary")),
                  null, // no timeout
                  Context.NONE);
      this.length = checksum.contentLength();
      final PathInfo info = resp.getValue();
      return new ADLSInputFile(
          location(),
          checksum.contentLength(),
          fileClient(),
          azureProperties(),
          metrics(),
          new DataLakeRequestConditions().setIfMatch(info.getETag()));
    } catch (DataLakeStorageException e) {
      if (412 == e.getStatusCode()) {
        // precondition failed
        throw new SupportsAtomicOperations.CASException("Target modified", e);
      }
      if (400 == e.getStatusCode() && e.getErrorCode().equals("InvalidFlushPosition")) {
        // spurious
        throw new SupportsAtomicOperations.CASException("Target modified", e);
      }
      if (409 == e.getStatusCode() && e.getErrorCode().equals("InvalidFlushOperation")) {
        // spurious
        throw new SupportsAtomicOperations.CASException("Target modified", e);
      }
      if (409 == e.getStatusCode() && e.getErrorCode().equals("PathAlreadyExists")) {
        // include among atomic errors
        throw new SupportsAtomicOperations.CASException("Location exists", e);
      }
      throw e;
    }
  }
}
