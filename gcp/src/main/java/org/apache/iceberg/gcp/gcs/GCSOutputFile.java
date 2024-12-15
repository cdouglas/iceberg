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
import java.io.OutputStream;
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
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;

class GCSOutputFile extends BaseGCSFile implements AtomicOutputFile<CAS> {

  static GCSOutputFile fromLocation(
      String location, Storage storage, GCPProperties gcpProperties, MetricsContext metrics) {
    return new GCSOutputFile(storage, BlobId.fromGsUtilUri(location), gcpProperties, metrics);
  }

  static GCSOutputFile fromBlobId(
      BlobId blobId, Storage storage, GCPProperties gcpProperties, MetricsContext metrics) {
    return new GCSOutputFile(storage, blobId, gcpProperties, metrics);
  }

  GCSOutputFile(
      Storage storage, BlobId blobId, GCPProperties gcpProperties, MetricsContext metrics) {
    super(storage, blobId, gcpProperties, metrics);
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
      // implicitly checks generation on the blobId, if specified
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
  public CAS prepare(Supplier<InputStream> source) {
    final GCSChecksum checksum = new GCSChecksum();
    // TODO replace with IOUtils (or whatever Guice provides)
    final byte[] buffer = new byte[8192];
    try (InputStream in = source.get();
        FileChecksumOutputStream chk =
            new FileChecksumOutputStream(
                new OutputStream() {
                  @Override
                  public void write(int b) {
                    // TODO: no NullOutputStream?
                  }

                  @Override
                  public void write(byte[] b, int off, int len) {
                    // do nothing
                  }
                },
                checksum)) {
      ByteStreams.copy(in, chk);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return checksum;
  }

  @Override
  public InputFile writeAtomic(CAS token, final Supplier<InputStream> source) throws IOException {
    final InputFile[] ret = new InputFile[1]; // Java. FFS.
    try (InputStream src = source.get()) {
      // TODO onClose is from a previous implementation; define a less clunky internal API
      try (PositionOutputStream dest =
          new GCSOutputStream(
              storage(), blobId(), gcpProperties(), metrics(), token, closed -> ret[0] = closed)) {
        byte[] buf = new byte[gcpProperties().channelWriteChunkSize().orElse(32 * 1024)];
        while (true) {
          int nread = src.read(buf);
          if (nread == -1) {
            break;
          }
          dest.write(buf, 0, nread);
        }
      }
    }
    return ret[0];
  }
}
