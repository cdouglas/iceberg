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
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.GrpcStorageOptions;
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
  private final Map<String, String> propertiesWithUserAgent;
  private SerializableSupplier<Storage> storage;
  private CloseableGroup closeableGroup;
  private transient volatile Storage storageClient;
  private transient volatile AutoCloseable gcsFileSystem;
  // CAS writer for zonal/Rapid buckets, built on first zonal-bucket write.
  private transient volatile GCSXmlPutWriter xmlPutWriter;
  // gRPC client for regular (streaming) writes to zonal/Rapid buckets, whose only streaming write
  // is an appendable upload. Built on the first such write.
  private transient volatile Storage grpcStorageClient;
  // Set when this PrefixedStorage builds its own clients; null with a caller-provided Storage.
  private final boolean ownsClients;

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
    this.propertiesWithUserAgent =
        ImmutableMap.<String, String>builder()
            .putAll(properties)
            .put("gcs.user-agent", GCS_FILE_IO_USER_AGENT)
            .build();
    this.closeableGroup = new CloseableGroup();
    this.ownsClients = null == storage;
    if (null == storage) {
      this.storage = () -> configureBuilder(StorageOptions.newBuilder()).build().getService();
    }
  }

  private StorageOptions.Builder configureBuilder(StorageOptions.Builder builder) {
    builder.setHeaderProvider(
        FixedHeaderProvider.create(ImmutableMap.of("User-agent", GCS_FILE_IO_USER_AGENT)));
    gcpProperties.projectId().ifPresent(builder::setProjectId);
    gcpProperties.clientLibToken().ifPresent(builder::setClientLibToken);
    gcpProperties.serviceHost().ifPresent(builder::setHost);

    Credentials credentials = credentials(gcpProperties);
    if (credentials != null) {
      builder.setCredentials(credentials);
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
   * A gRPC-transport client for appendable uploads to zonal/Rapid buckets. Configured like the HTTP
   * client when this PrefixedStorage builds its own; with a caller-provided Storage, it takes that
   * client's credentials and project.
   */
  Storage grpcStorage() {
    if (null == grpcStorageClient) {
      synchronized (this) {
        if (null == grpcStorageClient) {
          GrpcStorageOptions.Builder builder = StorageOptions.grpc();
          // The default-enabled gRPC OpenTelemetry metrics pull com.google.monitoring.v3 proto
          // classes that can collide with the protobuf classpath of bundled environments (e.g. the
          // YCSB binding) and throw ClassCastException while building the client. We don't
          // consume them.
          builder.setEnableGrpcClientMetrics(false);
          if (ownsClients) {
            configureBuilder(builder);
          } else {
            StorageOptions options = storage().getOptions();
            builder.setCredentials(options.getCredentials()).setProjectId(options.getProjectId());
          }
          this.grpcStorageClient = builder.build().getService();
        }
      }
    }
    return grpcStorageClient;
  }

  /** The single-request XML PUT writer used for CAS on zonal/Rapid buckets. */
  GCSXmlPutWriter xmlPutWriter() {
    if (null == xmlPutWriter) {
      synchronized (this) {
        if (null == xmlPutWriter) {
          this.xmlPutWriter = new GCSXmlPutWriter(storage(), gcpProperties);
        }
      }
    }
    return xmlPutWriter;
  }

  public GCPProperties gcpProperties() {
    return gcpProperties;
  }

  /**
   * Returns {@code true} if the named bucket is a zonal (Rapid Storage) bucket. The result is
   * cached per-bucket on first probe; bucket type is fixed at creation time, so caching is safe. If
   * the probe itself fails (auth, transient), returns {@code false} — keeping callers on the
   * standard-bucket write path.
   */
  public boolean isZonalBucket(String bucketName) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(bucketName), "Invalid bucket name: null or empty");
    return zonalCache.computeIfAbsent(bucketName, this::probeZonal);
  }

  private boolean probeZonal(String bucketName) {
    try {
      Bucket bucket = storage().get(bucketName, BucketGetOption.fields(BucketField.LOCATION_TYPE));
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
    try {
      try {
        if (null != closeableGroup) {
          closeableGroup.close();
        }
      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      } finally {
        if (null != gcsFileSystem) {
          AnalyticsCoreUtil.close(gcsFileSystem);
          gcsFileSystem = null;
        }
      }
    } finally {
      // The gRPC client holds channels; the HTTP client does not need closing.
      if (null != grpcStorageClient) {
        try {
          grpcStorageClient.close();
        } catch (Exception e) {
          LOG.debug("Closing gRPC Storage client failed; ignoring", e);
        }
        grpcStorageClient = null;
      }

      xmlPutWriter = null;
      if (null != storage) {
        // GCS Storage does not appear to be closable, so release the reference
        storage = null;
      }
    }
  }

  // Returns AutoCloseable to avoid a runtime dependency on gcs-analytics-core. Cast via
  // AnalyticsCoreUtil.
  AutoCloseable gcsFileSystem() {
    if (!gcpProperties.isGcsAnalyticsCoreEnabled()) {
      return null;
    }

    if (gcsFileSystem == null) {
      synchronized (this) {
        if (gcsFileSystem == null) {
          this.gcsFileSystem =
              AnalyticsCoreUtil.createFileSystem(
                  propertiesWithUserAgent, credentials(gcpProperties));
        }
      }
    }

    return gcsFileSystem;
  }

  private Credentials credentials(GCPProperties properties) {
    // Google Cloud APIs default to automatically detect the credentials to use, which is
    // in most cases the convenient way, especially in GCP.
    // See javadoc of com.google.auth.oauth2.GoogleCredentials.getApplicationDefault()
    if (properties.oauth2Token().isPresent()) {
      return GCPAuthUtils.oauth2CredentialsFromGcpProperties(properties, closeableGroup);
    } else if (properties.noAuth()) {
      // Explicitly allow "no credentials" for testing purposes
      return NoCredentials.getInstance();
    } else if (properties.impersonateServiceAccount().isPresent()) {
      return buildImpersonatedCredentials(properties);
    } else {
      return null;
    }
  }

  private Credentials buildImpersonatedCredentials(GCPProperties properties) {
    try {
      GoogleCredentials sourceCredentials = GoogleCredentials.getApplicationDefault();

      ImpersonatedCredentials impersonatedCredentials =
          ImpersonatedCredentials.create(
              sourceCredentials,
              properties.impersonateServiceAccount().get(),
              properties.impersonateDelegates(),
              properties.impersonateScopes(),
              properties.impersonateLifetimeSeconds());

      // Refresh to get initial token
      impersonatedCredentials.refresh();
      return impersonatedCredentials;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create impersonated credentials for GCS", e);
    }
  }
}
