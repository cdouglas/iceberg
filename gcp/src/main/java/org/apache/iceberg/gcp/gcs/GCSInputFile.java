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
import com.google.cloud.storage.Storage;
import java.io.IOException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GCSInputFile extends BaseGCSFile implements InputFile {
  private static final Logger LOG = LoggerFactory.getLogger(GCSInputFile.class);
  private Long blobSize;

  static GCSInputFile fromLocation(
      String location, PrefixedStorage storage, MetricsContext metrics) {
    return fromLocation(location, 0L, storage, metrics);
  }

  static GCSInputFile fromLocation(
      String location, long length, PrefixedStorage storage, MetricsContext metrics) {
    return new GCSInputFile(
        storage.storage(),
        storage.gcsFileSystem(),
        BlobId.fromGsUtilUri(location),
        length > 0 ? length : null,
        storage.gcpProperties(),
        metrics);
  }

  static GCSInputFile fromBlobId(
      BlobId blobId,
      Storage storage,
      AutoCloseable gcsFileSystem,
      GCPProperties gcpProperties,
      MetricsContext metrics) {
    return new GCSInputFile(storage, gcsFileSystem, blobId, null, gcpProperties, metrics);
  }

  static GCSInputFile fromBlobId(
      BlobId blobId,
      long length,
      Storage storage,
      AutoCloseable gcsFileSystem,
      GCPProperties gcpProperties,
      MetricsContext metrics) {
    return new GCSInputFile(
        storage, gcsFileSystem, blobId, length > 0 ? length : null, gcpProperties, metrics);
  }

  GCSInputFile(
      Storage storage,
      // Using AutoCloseable avoids a runtime dependency on gcs-analytics-core. Cast via
      // AnalyticsCoreUtil.
      AutoCloseable gcsFileSystem,
      BlobId blobId,
      Long blobSize,
      GCPProperties gcpProperties,
      MetricsContext metrics) {
    super(storage, gcsFileSystem, blobId, gcpProperties, metrics);
    this.blobSize = blobSize;
  }

  @Override
  public long getLength() {
    if (blobSize == null) {
      this.blobSize = getBlob().getSize();
    }

    return blobSize;
  }

  @Override
  public SeekableInputStream newStream() {
    if (gcpProperties().isGcsAnalyticsCoreEnabled()) {
      try {
        return AnalyticsCoreUtil.newStream(gcsFileSystem(), blobId(), blobSize, metrics());
      } catch (IOException e) {
        LOG.error(
            "Failed to create GCS analytics core input stream for {}, falling back to default.",
            uri(),
            e);
      }
    }

    return new GCSInputStream(storage(), blobId(), blobSize, gcpProperties(), metrics());
  }

  /**
   * Resolve this InputFile to a snapshot BlobId for use as a CAS precondition, without a request:
   * the generation it carries, else the one a lookup saw.
   *
   * <p>Always returns a BlobId with a generation set, suitable for {@link
   * com.google.cloud.storage.Storage.BlobWriteOption#generationMatch(long)}: a positive value pins
   * the generation the snapshot has seen, {@code 0} pins "object must not exist", which a snapshot
   * that has seen none assumes. This mirrors {@code ADLSInputFile.conditions()}, which returns a
   * {@code DataLakeRequestConditions} encoding {@code ifMatch=etag}, or null for a snapshot that
   * has seen no version, written with {@code ifNoneMatch=*}.
   */
  BlobId pinnedBlobId() {
    if (blobId().getGeneration() != null) {
      return blobId();
    }
    Blob blob = cachedBlob();
    return blob == null
        ? BlobId.of(blobId().getBucket(), blobId().getName(), 0L)
        : blob.getBlobId();
  }
}
