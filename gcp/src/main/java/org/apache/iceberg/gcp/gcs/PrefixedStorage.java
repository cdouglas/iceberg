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

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BucketField;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.gcp.GCPAuthUtils;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PrefixedStorage implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(PrefixedStorage.class);
  private static final String GCS_FILE_IO_USER_AGENT = "gcsfileio/" + EnvironmentContext.get();
  private static final String ZONAL_LOCATION_TYPE = "zone";
  private final String storagePrefix;
  private final GCPProperties gcpProperties;
  private SerializableSupplier<Storage> storage;
  // Separate gRPC client built lazily on first zonal-bucket access. The default HTTP transport
  // works for everything on standard buckets; zonal Rapid buckets require gRPC for the writes
  // (blobAppendableUpload + moveBlob) and a second client lets us keep the HTTP read path intact.
  // Built only when a caller observes a zonal bucket; null otherwise.
  private SerializableSupplier<Storage> grpcStorageSupplier;
  private CloseableGroup closeableGroup;
  private transient volatile Storage storageClient;
  private transient volatile Storage grpcStorageClient;

  // Bucket type (zonal/Rapid vs standard/regional) is fixed for the bucket's lifetime; probe once
  // per bucket via storage.get(bucket).getLocationType() and cache. A probe failure (auth,
  // transient) defaults the bucket to "not zonal" so the standard write path is preserved on
  // configuration error — the alternative would silently route a standard write through the more
  // expensive zonal path.
  private final ConcurrentMap<String, Boolean> zonalCache = new ConcurrentHashMap<>();

  PrefixedStorage(
      String storagePrefix, Map<String, String> properties, SerializableSupplier<Storage> storage) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(storagePrefix), "Invalid storage prefix: null or empty");
    Preconditions.checkArgument(null != properties, "Invalid properties: null");
    this.storagePrefix = storagePrefix;
    this.storage = storage;
    this.gcpProperties = new GCPProperties(properties);

    if (null == storage) {
      this.storage = () -> configureBuilder(StorageOptions.newBuilder()).build().getService();
      this.grpcStorageSupplier =
          () -> configureBuilder(StorageOptions.grpc()).build().getService();
    }
  }

  private StorageOptions.Builder configureBuilder(StorageOptions.Builder builder) {
    builder.setHeaderProvider(
        FixedHeaderProvider.create(ImmutableMap.of("User-agent", GCS_FILE_IO_USER_AGENT)));
    gcpProperties.projectId().ifPresent(builder::setProjectId);
    gcpProperties.clientLibToken().ifPresent(builder::setClientLibToken);
    gcpProperties.serviceHost().ifPresent(builder::setHost);

    // Google Cloud APIs default to automatically detect the credentials to use, which is
    // in most cases the convenient way, especially in GCP.
    // See javadoc of com.google.auth.oauth2.GoogleCredentials.getApplicationDefault()
    if (gcpProperties.noAuth()) {
      // Explicitly allow "no credentials" for testing purposes
      builder.setCredentials(NoCredentials.getInstance());
    }

    if (gcpProperties.oauth2Token().isPresent()) {
      if (null == closeableGroup) {
        closeableGroup = new CloseableGroup();
      }
      builder.setCredentials(
          GCPAuthUtils.oauth2CredentialsFromGcpProperties(gcpProperties, closeableGroup));
    }
    return builder;
  }

  public String storagePrefix() {
    return storagePrefix;
  }

  public Storage storage() {
    if (null == storageClient) {
      synchronized (this) {
        if (null == storageClient) {
          this.storageClient = storage.get();
        }
      }
    }

    return storageClient;
  }

  /**
   * Returns a gRPC-transport {@link Storage} client for zonal/Rapid buckets, which require the
   * appendable-objects API (gRPC-only) for any write. Lazily constructed on first call. Throws if
   * this PrefixedStorage was built with a caller-provided {@code Storage} supplier (we can't infer
   * the right gRPC client from a hand-constructed one).
   */
  public Storage grpcStorage() {
    if (null == grpcStorageClient) {
      synchronized (this) {
        if (null == grpcStorageClient) {
          Preconditions.checkState(
              null != grpcStorageSupplier,
              "gRPC Storage client is required for zonal-bucket writes but is unavailable. "
                  + "This PrefixedStorage was constructed with a caller-provided Storage instance; "
                  + "to use Rapid Storage buckets, let PrefixedStorage build its own clients.");
          this.grpcStorageClient = grpcStorageSupplier.get();
        }
      }
    }
    return grpcStorageClient;
  }

  public GCPProperties gcpProperties() {
    return gcpProperties;
  }

  /**
   * Returns {@code true} if the named bucket is a zonal (Rapid Storage) bucket. The result is
   * cached per-bucket on first probe; bucket type is fixed at creation time, so caching is safe.
   * If the probe itself fails (auth, transient), returns {@code false} — keeping callers on the
   * standard-bucket write path.
   */
  public boolean isZonalBucket(String bucketName) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(bucketName), "Invalid bucket name: null or empty");
    return zonalCache.computeIfAbsent(bucketName, this::probeZonal);
  }

  private boolean probeZonal(String bucketName) {
    try {
      Bucket bucket =
          storage().get(bucketName, BucketGetOption.fields(BucketField.LOCATION_TYPE));
      if (bucket == null) {
        LOG.debug("Bucket {} not found during zonal probe; defaulting to non-zonal", bucketName);
        return false;
      }
      return ZONAL_LOCATION_TYPE.equalsIgnoreCase(bucket.getLocationType());
    } catch (StorageException e) {
      LOG.warn(
          "Zonal-bucket probe for {} failed (code={}); defaulting to non-zonal",
          bucketName,
          e.getCode(),
          e);
      return false;
    }
  }

  @Override
  public void close() {
    if (null != closeableGroup) {
      try {
        closeableGroup.close();
      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      }
    }

    // The gRPC client supports close(); the HTTP client does not.
    if (null != grpcStorageClient) {
      try {
        grpcStorageClient.close();
      } catch (Exception e) {
        LOG.debug("Closing gRPC Storage client failed; ignoring", e);
      }
      grpcStorageClient = null;
    }

    if (null != storage) {
      // GCS Storage does not appear to be closable, so release the reference
      storage = null;
    }
    grpcStorageSupplier = null;
  }
}
