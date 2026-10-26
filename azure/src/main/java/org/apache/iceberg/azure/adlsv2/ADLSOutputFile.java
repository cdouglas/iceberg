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

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.AppendBlobCreateOptions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.SpecializedBlobClientBuilder;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.PathProperties;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;
import java.util.function.Function;
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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Output file for ADLS Gen2 (hierarchical namespace). Atomic writes keep the file as an Append
 * Blob:
 *
 * <ul>
 *   <li><b>APPEND</b> is one {@code AppendBlock} conditioned on the snapshot's length ({@code
 *       x-ms-blob-condition-appendpos}) and ETag ({@code If-Match}), carrying the block's MD5. The
 *       service checks both conditions and applies the block in one step, so of N writers appending
 *       to the same snapshot exactly one commits, and the committed bytes are its own.
 *   <li><b>CAS</b> writes the new content to a temp Append Blob next to the target, then renames it
 *       onto the target with the snapshot's ETag (or {@code If-None-Match: *}) as destination
 *       condition. Rename is atomic on a hierarchical-namespace account: readers see the old file
 *       or the new one, and a writer that fails before the rename leaves the target untouched.
 * </ul>
 *
 * <p>This replaced the Data Lake {@code uploadWithResponse} CAS and the leased append + flush.
 * {@code uploadWithResponse} is three requests (create, which truncates the file, then append and
 * flush, only the first conditional), so a writer that died after the create left an empty file,
 * and readers could observe one mid-write. Data Lake append + flush lets concurrent writers
 * overwrite each other's uncommitted bytes, which the lease papered over. A file written through
 * the Data Lake API cannot be appended with {@code AppendBlock} (409 {@code InvalidBlobType}); a
 * CAS replaces it with an Append Blob.
 *
 * <p>Under SAS auth the rename has to carry the SAS in {@code x-ms-rename-source}. The SDK adds it
 * for clients built with a SAS token ({@code adls.sas-token.*}); {@link
 * VendedAzureSasCredentialPolicy} adds the vended one. Shared key and OAuth need nothing.
 */
class ADLSOutputFile extends BaseADLSFile implements AtomicOutputFile {
  private static final Logger LOG = LoggerFactory.getLogger(ADLSOutputFile.class);

  private Long length;
  private final DataLakeRequestConditions conditions;
  // The snapshot had seen no version: a CAS asserts the file's absence, and an append extends the
  // file's current version, looked up when it writes
  private final boolean appendLooksUp;
  // Builds the Data Lake client for another location in the same file system: CAS stages its
  // temp next to the target. Clients built through ADLSFileIO carry the SAS the rename needs.
  private final Function<String, DataLakeFileClient> clients;

  ADLSOutputFile(
      String location,
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      MetricsContext metrics,
      Function<String, DataLakeFileClient> clients) {
    this(location, fileClient, azureProperties, 0, null, metrics, clients);
  }

  ADLSOutputFile(
      String location,
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      long length,
      DataLakeRequestConditions conditions,
      MetricsContext metrics,
      Function<String, DataLakeFileClient> clients) {
    this(location, fileClient, azureProperties, length, conditions, metrics, clients, false);
  }

  ADLSOutputFile(
      String location,
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      long length,
      DataLakeRequestConditions conditions,
      MetricsContext metrics,
      Function<String, DataLakeFileClient> clients,
      boolean appendLooksUp) {
    super(location, fileClient, azureProperties, metrics);
    this.length = length;
    this.conditions = conditions;
    this.clients = clients;
    this.appendLooksUp = appendLooksUp;
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

  /**
   * One {@code AppendBlock} at the snapshot's length, conditioned on the snapshot's ETag; for a
   * snapshot that had seen no version, the file's current ETag (and length, unless it had one).
   */
  private ADLSInputFile appendDestObj(ADLSChecksum checksum, Supplier<InputStream> source)
      throws IOException {
    String pinned = conditions == null ? null : conditions.getIfMatch();
    long at = length;
    if (appendLooksUp) {
      PathProperties current = currentFile();
      pinned = current.getETag();
      at = at >= 0 ? at : current.getFileSize();
    }
    if (pinned == null || at < 0) {
      throw new SupportsAtomicOperations.AppendException(
          "Cannot append to " + location() + ": no snapshot of an existing file", null);
    }
    final long appendLen = checksum.contentLength();
    try (InputStream src = source.get()) {
      String etag =
          appendBlob(fileClient())
              .appendBlockWithResponse(
                  src,
                  appendLen,
                  // The service rejects the block (400 Md5Mismatch) if the bytes sent are not the
                  // ones prepare() checksummed.
                  checksum.contentChecksumBytes(),
                  new AppendBlobRequestConditions().setAppendPosition(at).setIfMatch(pinned),
                  null,
                  Context.NONE)
              .getValue()
              .getETag();
      this.length = at + appendLen;
      return snapshot(length, etag);
    } catch (BlobStorageException e) {
      if (412 == e.getStatusCode()) {
        // ConditionNotMet (ETag) or AppendPositionConditionNotMet (length): another writer won
        throw new SupportsAtomicOperations.AppendException("Target modified", e);
      }
      if (404 == e.getStatusCode()) {
        throw new SupportsAtomicOperations.AppendException("Target no longer exists", e);
      }
      if (409 == e.getStatusCode()
          && BlobErrorCode.BLOCK_COUNT_EXCEEDS_LIMIT.equals(e.getErrorCode())) {
        // an Append Blob holds 50,000 blocks; a CAS replaces it
        throw new SupportsAtomicOperations.SegmentLimitException(
            "Cannot append to " + location() + ": the blob holds the most blocks Azure allows", e);
      }
      // 409 InvalidBlobType (written through the Data Lake API; a CAS converts it) propagates as it
      // is: retrying the append cannot succeed.
      throw e;
    }
  }

  /**
   * Stage the content in a temp Append Blob next to the target, then rename it onto the target with
   * the snapshot's ETag as the destination condition.
   */
  private ADLSInputFile replaceDestObj(ADLSChecksum checksum, Supplier<InputStream> source)
      throws IOException {
    Preconditions.checkState(
        clients != null, "Atomic writes need a client factory: %s", location());
    DataLakeFileClient temp = clients.apply(tempLocation());
    AppendBlobClient tempBlob = appendBlob(temp);
    boolean renamed = false;
    try {
      String etag = stage(tempBlob, checksum, source);
      DataLakeRequestConditions destination = null;
      if (conditions != null) {
        destination =
            new DataLakeRequestConditions()
                .setIfMatch(conditions.getIfMatch())
                .setIfNoneMatch(conditions.getIfNoneMatch());
      }
      Response<DataLakeFileClient> resp =
          temp.renameWithResponse(
              null,
              fileClient().getFilePath(),
              new DataLakeRequestConditions().setIfMatch(etag),
              destination,
              null,
              Context.NONE);
      renamed = true;
      // The rename keeps the temp's ETag; the response carries it too. Never re-read it: a HEAD
      // after the rename could see a later writer's ETag.
      String renamedEtag = resp.getHeaders().getValue(HttpHeaderName.ETAG);
      this.length = checksum.contentLength();
      return snapshot(length, renamedEtag != null ? renamedEtag : etag);
    } catch (DataLakeStorageException e) {
      if (412 == e.getStatusCode()) {
        // ConditionNotMet on the destination: the target moved on
        throw new SupportsAtomicOperations.CASException("Target modified", e);
      }
      if (409 == e.getStatusCode() && "PathAlreadyExists".equals(e.getErrorCode())) {
        throw new SupportsAtomicOperations.CASException("Location exists", e);
      }
      throw e;
    } finally {
      if (!renamed) {
        try {
          tempBlob.deleteIfExists();
        } catch (RuntimeException e) {
          LOG.warn("Failed to delete CAS temp {}", temp.getFilePath(), e);
        }
      }
    }
  }

  /**
   * Write the content to a new Append Blob, in blocks no larger than the service accepts, and check
   * that it is what {@code prepare()} checksummed. Returns the blob's ETag.
   */
  private String stage(AppendBlobClient blob, ADLSChecksum checksum, Supplier<InputStream> source)
      throws IOException {
    String etag =
        blob.createWithResponse(
                new AppendBlobCreateOptions()
                    .setRequestConditions(new BlobRequestConditions().setIfNoneMatch("*")),
                null,
                Context.NONE)
            .getValue()
            .getETag();
    MessageDigest whole = md5();
    long written = 0;
    byte[] buf =
        new byte
            [(int) Math.max(1, Math.min(blob.getMaxAppendBlockBytes(), checksum.contentLength()))];
    try (InputStream src = source.get()) {
      int n;
      while ((n = ByteStreams.read(src, buf, 0, buf.length)) > 0) {
        whole.update(buf, 0, n);
        MessageDigest block = md5();
        block.update(buf, 0, n);
        etag =
            blob.appendBlockWithResponse(
                    new ByteArrayInputStream(buf, 0, n),
                    n,
                    block.digest(),
                    new AppendBlobRequestConditions().setAppendPosition(written).setIfMatch(etag),
                    null,
                    Context.NONE)
                .getValue()
                .getETag();
        written += n;
      }
    }
    if (written != checksum.contentLength()
        || !Arrays.equals(whole.digest(), checksum.contentChecksumBytes())) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "Source for %s changed since prepare(): %d bytes, prepared %d",
              location(),
              written,
              checksum.contentLength()));
    }
    return etag;
  }

  /** The file an append to a snapshot that had seen no version extends: its current version. */
  private PathProperties currentFile() {
    try {
      return fileClient().getProperties();
    } catch (DataLakeStorageException e) {
      if (404 == e.getStatusCode()) {
        throw new SupportsAtomicOperations.AppendException(
            "Cannot append to " + location() + ": no file", e);
      }
      throw e;
    }
  }

  private ADLSInputFile snapshot(long len, String etag) {
    return new ADLSInputFile(
        location(),
        len,
        fileClient(),
        azureProperties(),
        metrics(),
        new DataLakeRequestConditions().setIfMatch(etag));
  }

  // A hidden sibling of the target: same directory, so the rename stays within it.
  private String tempLocation() {
    String loc = location();
    int slash = loc.lastIndexOf('/');
    return loc.substring(0, slash + 1)
        + "."
        + loc.substring(slash + 1)
        + ".cas-"
        + UUID.randomUUID()
        + ".tmp";
  }

  /**
   * The Blob API client for a Data Lake file: same pipeline (credentials, policies, retries), blob
   * endpoint. Append Blob writes go through the Blob API; the Data Lake API has no equivalent.
   */
  static AppendBlobClient appendBlob(DataLakeFileClient file) {
    return new SpecializedBlobClientBuilder()
        .pipeline(file.getHttpPipeline())
        .endpoint(file.getFileUrl().replaceFirst("^(https?://[^/]*?)\\.dfs\\.", "$1.blob."))
        .buildAppendBlobClient();
  }

  private static MessageDigest md5() {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("Missing MD5 implementation", e);
    }
  }
}
