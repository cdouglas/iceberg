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
package org.apache.iceberg.gcp.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.FileChecksumOutputStream;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;

class GCSOutputFile extends BaseGCSFile implements AtomicOutputFile {

  // Pinned snapshot of the InputFile being replaced (null for vanilla overwrites). The BlobId's
  // generation drives the write precondition: a positive value enforces generationMatch on the
  // captured generation; 0 enforces "object must not exist".
  private final BlobId pinnedSnapshot;

  // Non-null when the target is a zonal/Rapid bucket, which rejects the JSON-API create:
  // writeAtomic sends one XML API PUT instead. Null on standard buckets, which write with one
  // storage.create request (createAtomic).
  private final GCSXmlPutWriter zonalWriter;

  // Non-null when the target is a zonal/Rapid bucket, which rejects resumable uploads:
  // createOrOverwrite streams through an appendable upload on this gRPC client instead.
  private final Supplier<Storage> appendableStorage;

  static GCSOutputFile fromLocation(
      String location, PrefixedStorage storage, MetricsContext metrics) {
    BlobId blobId = BlobId.fromGsUtilUri(location);
    return new GCSOutputFile(
        storage.storage(),
        storage.gcsFileSystem(),
        blobId,
        storage.gcpProperties(),
        metrics,
        null,
        null,
        storage.isZonalBucket(blobId.getBucket()) ? storage::grpcStorage : null);
  }

  static GCSOutputFile fromBlobId(
      BlobId blobId,
      Storage storage,
      AutoCloseable gcsFileSystem,
      GCPProperties gcpProperties,
      MetricsContext metrics) {
    return new GCSOutputFile(
        storage, gcsFileSystem, blobId, gcpProperties, metrics, null, null, null);
  }

  /**
   * Build an OutputFile that replaces an InputFile snapshot.
   *
   * @param target write destination (bucket+name, no generation)
   * @param pinnedSnapshot snapshot BlobId with generation set; positive pins the existing
   *     generation, {@code 0} pins "object must not exist"
   * @param zonalWriter non-null when the target's bucket is zonal/Rapid; routes {@code writeAtomic}
   *     through a single XML API PUT. Null on standard buckets (single-request create).
   * @param appendableStorage non-null when the target's bucket is zonal/Rapid; {@code
   *     createOrOverwrite} streams through an appendable upload on it
   */
  static GCSOutputFile replacing(
      BlobId target,
      BlobId pinnedSnapshot,
      Storage storage,
      AutoCloseable gcsFileSystem,
      GCPProperties gcpProperties,
      MetricsContext metrics,
      GCSXmlPutWriter zonalWriter,
      Supplier<Storage> appendableStorage) {
    return new GCSOutputFile(
        storage,
        gcsFileSystem,
        target,
        gcpProperties,
        metrics,
        pinnedSnapshot,
        zonalWriter,
        appendableStorage);
  }

  GCSOutputFile(
      Storage storage,
      // Using AutoCloseable avoids a runtime dependency on gcs-analytics-core. Cast via
      // AnalyticsCoreUtil.
      AutoCloseable gcsFileSystem,
      BlobId blobId,
      GCPProperties gcpProperties,
      MetricsContext metrics,
      BlobId pinnedSnapshot,
      GCSXmlPutWriter zonalWriter,
      Supplier<Storage> appendableStorage) {
    super(storage, gcsFileSystem, blobId, gcpProperties, metrics);
    this.pinnedSnapshot = pinnedSnapshot;
    this.zonalWriter = zonalWriter;
    this.appendableStorage = appendableStorage;
  }

  /**
   * Create an output stream for the specified location if the target object does not exist in GCS
   * at the time of invocation.
   *
   * @return output stream
   */
  @Override
  public PositionOutputStream create() {
    if (!exists()) {
      return createOrOverwrite();
    } else {
      throw new AlreadyExistsException("Location already exists: %s", uri());
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    try {
      if (appendableStorage != null) {
        return new GCSAppendableOutputStream(
            appendableStorage.get(), blobId(), gcpProperties(), metrics());
      }
      return new GCSOutputStream(storage(), blobId(), gcpProperties(), metrics());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create output stream for location: " + uri(), e);
    }
  }

  @Override
  public InputFile toInputFile() {
    return new GCSInputFile(storage(), gcsFileSystem(), blobId(), null, gcpProperties(), metrics());
  }

  @Override
  public CAS prepare(Supplier<InputStream> source, Strategy howto) throws IOException {
    // GCS objects are immutable -- every write replaces the whole object. There is no
    // append primitive (no Azure AppendBlob analog, no S3 byte-range append), so the
    // only honest implementation of Strategy.APPEND would be a CAS replace, which is
    // already what Strategy.CAS does. Reject APPEND up front so callers that depend on
    // append semantics fail loudly instead of silently overwriting the object.
    Preconditions.checkArgument(
        howto == Strategy.CAS,
        "GCS does not support append-mode atomic writes; use Strategy.CAS (got %s)",
        howto);
    final GCSChecksum checksum = new GCSChecksum();
    try (InputStream in = source.get();
        FileChecksumOutputStream chk =
            new FileChecksumOutputStream(ByteStreams.nullOutputStream(), checksum)) {
      ByteStreams.copy(in, chk);
    }
    return checksum;
  }

  @Override
  public InputFile writeAtomic(CAS token, final Supplier<InputStream> source) throws IOException {
    // Every path writes from a buffer: prepare() has already read the source once to checksum
    // it, the catalog holds the payload in memory anyway, and a single-request upload needs the
    // bytes up front. Re-check the checksum here, because the SDK computes its own CRC32C over
    // the bytes it sends; a source that changed since prepare() must not be written.
    final byte[] payload;
    try (InputStream src = source.get()) {
      payload = ByteStreams.toByteArray(src);
    }
    GCSChecksum actual = new GCSChecksum();
    actual.update(payload, 0, payload.length);
    if (actual.contentLength() != ((GCSChecksum) token).contentLength()
        || !actual.contentHeaderString().equals(token.contentHeaderString())) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "Source for %s changed since prepare(): CRC32C %s (%d bytes), prepared %s (%d bytes)",
              uri(),
              actual.contentHeaderString(),
              actual.contentLength(),
              token.contentHeaderString(),
              ((GCSChecksum) token).contentLength()));
    }
    if (zonalWriter != null) {
      return zonalWriter.writeAtomic(
          blobId(),
          pinnedSnapshot,
          token.contentHeaderString(),
          payload,
          gcsFileSystem(),
          metrics());
    }
    return createAtomic(payload);
  }

  /**
   * Standard buckets: one multipart upload ({@code objects.insert}) carrying the generation
   * precondition. It returns the new object's metadata, generation included, so no second request
   * is needed to pin the result.
   */
  private InputFile createAtomic(byte[] payload) {
    List<BlobTargetOption> options = Lists.newArrayList();
    gcpProperties()
        .encryptionKey()
        .ifPresent(key -> options.add(BlobTargetOption.encryptionKey(key)));
    gcpProperties()
        .userProject()
        .ifPresent(project -> options.add(BlobTargetOption.userProject(project)));
    // The pinned generation drives the precondition: 0 requires that the object not exist,
    // positive pins the captured live generation. Null means no precondition (plain overwrite).
    if (pinnedSnapshot != null) {
      options.add(BlobTargetOption.generationMatch(pinnedSnapshot.getGeneration()));
    }
    try {
      Blob written =
          storage()
              .create(
                  BlobInfo.newBuilder(blobId()).build(),
                  payload,
                  options.toArray(new BlobTargetOption[0]));
      metrics().counter(FileIOMetricsContext.WRITE_BYTES, Unit.BYTES).increment(payload.length);
      metrics().counter(FileIOMetricsContext.WRITE_OPERATIONS).increment();
      return new GCSInputFile(
          storage(),
          gcsFileSystem(),
          written.getBlobId(),
          written.getSize(),
          gcpProperties(),
          metrics());
    } catch (StorageException e) {
      throw translate(e);
    }
  }

  /** Maps a failed conditional write onto the {@link SupportsAtomicOperations} exceptions. */
  static RuntimeException translate(StorageException e) {
    int code = e.getCode();
    if (code == 412) {
      // https://cloud.google.com/storage/docs/json_api/v1/status-codes#412_Precondition_Failed
      return new SupportsAtomicOperations.StorageInvariantException("Target modified", e);
    }
    if (code == 429 || (code >= 500 && code < 600)) {
      // 429 = per-object mutation rate (about one write per second per object on standard
      // buckets) or quota; 5xx = transient. Both are recoverable by retrying after a backoff.
      return new SupportsAtomicOperations.StorageThrottleException(
          "Storage applied backpressure (HTTP " + code + ")", e);
    }
    // Other codes (auth, malformed request) propagate as the original StorageException.
    return e;
  }
}
