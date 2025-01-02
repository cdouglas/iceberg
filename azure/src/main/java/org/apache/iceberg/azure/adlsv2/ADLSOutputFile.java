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
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.models.PathInfo;
import com.azure.storage.file.datalake.options.FileParallelUploadOptions;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
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

class ADLSOutputFile extends BaseADLSFile implements AtomicOutputFile<CAS> {

  private Long length;
  private final DataLakeRequestConditions conditions;

  ADLSOutputFile(
      String location,
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      MetricsContext metrics) {
    this(location, fileClient, azureProperties, null, metrics);
  }

  ADLSOutputFile(
      String location,
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      DataLakeRequestConditions conditions,
      MetricsContext metrics) {
    super(location, fileClient, azureProperties, metrics);
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
    // !#! TODO update this.length
    return new ADLSOutputStream(fileClient(), azureProperties(), metrics());
  }

  @Override
  public InputFile toInputFile() {
    return new ADLSInputFile(
        location(), length, fileClient(), azureProperties(), metrics(), conditions);
  }

  @Override
  public CAS prepare(Supplier<InputStream> source, Strategy howto) {
    final ADLSChecksum checksum = new ADLSChecksum(howto);
    final byte[] buffer = new byte[8192];
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
    // TODO this is very ugly. clean it up later.
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

  private ADLSInputFile appendDestObj(ADLSChecksum checksum, Supplier<InputStream> source)
      throws IOException {
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
      throw e;
    }
  }

  private ADLSInputFile replaceDestObj(ADLSChecksum checksum, Supplier<InputStream> source)
      throws IOException {
    // Annoyingly, the checksum is not validated server-side, but stored as metadata. The
    // partial-write problem is less of an issue using an InputStream, so the length validation can
    // suffice
    try (InputStream src = source.get()) {
      // TODO local runner doesn't support this API, testcontainer path will fail
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
      throw e;
    }
  }
}
