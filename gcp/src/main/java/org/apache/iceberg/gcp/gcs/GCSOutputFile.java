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
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.FileChecksumOutputStream;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;

class GCSOutputFile extends BaseGCSFile implements AtomicOutputFile {

  // Pinned snapshot of the InputFile being replaced (null for vanilla overwrites). The BlobId's
  // generation drives the write precondition: a positive value enforces generationMatch on the
  // captured generation; 0 enforces "object must not exist".
  private final BlobId pinnedSnapshot;

  static GCSOutputFile fromLocation(
      String location, PrefixedStorage storage, MetricsContext metrics) {
    return new GCSOutputFile(
        storage.storage(), BlobId.fromGsUtilUri(location), storage.gcpProperties(), metrics, null);
  }

  static GCSOutputFile fromBlobId(
      BlobId blobId, Storage storage, GCPProperties gcpProperties, MetricsContext metrics) {
    return new GCSOutputFile(storage, blobId, gcpProperties, metrics, null);
  }

  /**
   * Build an OutputFile that replaces an InputFile snapshot.
   *
   * @param target write destination (bucket+name, no generation)
   * @param pinnedSnapshot snapshot BlobId with generation set; positive pins the existing
   *     generation, {@code 0} pins "object must not exist"
   */
  static GCSOutputFile replacing(
      BlobId target,
      BlobId pinnedSnapshot,
      Storage storage,
      GCPProperties gcpProperties,
      MetricsContext metrics) {
    return new GCSOutputFile(storage, target, gcpProperties, metrics, pinnedSnapshot);
  }

  GCSOutputFile(
      Storage storage,
      BlobId blobId,
      GCPProperties gcpProperties,
      MetricsContext metrics,
      BlobId pinnedSnapshot) {
    super(storage, blobId, gcpProperties, metrics);
    this.pinnedSnapshot = pinnedSnapshot;
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
    // GCS enforces a per-object update rate (~1/sec sustained); under bursty workloads
    // close() surfaces 429 from getResult(). 429 (and 5xx) are transient -- retry the
    // entire write with capped exponential backoff and jitter. Buffer the source bytes
    // up front because the supplier given by the caller is not guaranteed to replay
    // (e.g. ProtoCatalogFormat passes a single ByteArrayInputStream that is exhausted
    // by the first attempt).
    final byte[] payload;
    try (InputStream src = source.get()) {
      payload = ByteStreams.toByteArray(src);
    }

    final int maxAttempts = 8;
    long backoffMs = 200L;
    StorageException lastTransient = null;
    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        return writeAtomicOnce(token, () -> new java.io.ByteArrayInputStream(payload));
      } catch (StorageException e) {
        if (!isTransient(e)) {
          throw e;
        }
        lastTransient = e;
        long sleepMs = backoffMs + ThreadLocalRandom.current().nextLong(backoffMs / 2 + 1);
        try {
          Thread.sleep(sleepMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted retrying atomic write to: " + uri(), ie);
        }
        backoffMs = Math.min(backoffMs * 2, 5_000L);
      }
    }
    throw new IOException(
        "Exceeded " + maxAttempts + " transient retries on atomic write to: " + uri(),
        lastTransient);
  }

  private InputFile writeAtomicOnce(CAS token, Supplier<InputStream> source) throws IOException {
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

  private static boolean isTransient(StorageException e) {
    int code = e.getCode();
    return code == 429 || (code >= 500 && code < 600);
  }
}
