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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.FileChecksumOutputStream;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;

class GCSOutputFile extends BaseGCSFile implements AtomicOutputFile {

  // Pinned snapshot of the InputFile being replaced (null for vanilla overwrites). The BlobId's
  // generation drives the write precondition: a positive value enforces generationMatch on the
  // captured generation; 0 enforces "object must not exist".
  private final BlobId pinnedSnapshot;

  // Non-null when the target is a zonal/Rapid bucket: writeAtomic routes through stage-and-move on
  // a gRPC-transport Storage client. Null on standard buckets — they keep the direct
  // blobWriteSession path through GCSAtomicOutputStream.
  private final GCSRapidStageAndMove zonalWriter;

  static GCSOutputFile fromLocation(
      String location, PrefixedStorage storage, MetricsContext metrics) {
    return new GCSOutputFile(
        storage.storage(),
        BlobId.fromGsUtilUri(location),
        storage.gcpProperties(),
        metrics,
        null,
        null);
  }

  static GCSOutputFile fromBlobId(
      BlobId blobId, Storage storage, GCPProperties gcpProperties, MetricsContext metrics) {
    return new GCSOutputFile(storage, blobId, gcpProperties, metrics, null, null);
  }

  /**
   * Build an OutputFile that replaces an InputFile snapshot.
   *
   * @param target write destination (bucket+name, no generation)
   * @param pinnedSnapshot snapshot BlobId with generation set; positive pins the existing
   *     generation, {@code 0} pins "object must not exist"
   * @param zonalWriter non-null when the target's bucket is zonal/Rapid; routes {@code writeAtomic}
   *     through stage-and-move. Null on standard buckets (existing direct-write path).
   */
  static GCSOutputFile replacing(
      BlobId target,
      BlobId pinnedSnapshot,
      Storage storage,
      GCPProperties gcpProperties,
      MetricsContext metrics,
      GCSRapidStageAndMove zonalWriter) {
    return new GCSOutputFile(storage, target, gcpProperties, metrics, pinnedSnapshot, zonalWriter);
  }

  GCSOutputFile(
      Storage storage,
      BlobId blobId,
      GCPProperties gcpProperties,
      MetricsContext metrics,
      BlobId pinnedSnapshot,
      GCSRapidStageAndMove zonalWriter) {
    super(storage, blobId, gcpProperties, metrics);
    this.pinnedSnapshot = pinnedSnapshot;
    this.zonalWriter = zonalWriter;
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
      return new GCSOutputStream(storage(), blobId(), gcpProperties(), metrics());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create output stream for location: " + uri(), e);
    }
  }

  @Override
  public InputFile toInputFile() {
    return new GCSInputFile(storage(), blobId(), null, gcpProperties(), metrics());
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
    if (zonalWriter != null) {
      // Zonal/Rapid: stage-and-move. The full payload is buffered into memory first so the temp
      // upload is a single atomic write (no partial-bytes exposure on Rapid's appendable API).
      byte[] payload;
      try (InputStream src = source.get()) {
        payload = ByteStreams.toByteArray(src);
      }
      return zonalWriter.writeAtomic(blobId(), pinnedSnapshot, token.contentHeaderString(), payload);
    }
    // Standard buckets: existing direct-write path through blobWriteSession.
    final InputFile[] result = new InputFile[1];
    try (InputStream src = source.get()) {
      try (GCSAtomicOutputStream dest =
          new GCSAtomicOutputStream(
              storage(),
              blobId(),
              gcpProperties(),
              metrics(),
              token,
              pinnedSnapshot,
              written -> result[0] = written)) {
        byte[] buf = new byte[gcpProperties().channelWriteChunkSize().orElse(32 * 1024)];
        int nread;
        while ((nread = src.read(buf)) != -1) {
          dest.write(buf, 0, nread);
        }
      }
    }
    return result[0];
  }
}
