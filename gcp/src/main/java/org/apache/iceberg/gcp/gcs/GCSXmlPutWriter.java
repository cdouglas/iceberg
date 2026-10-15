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

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * CAS-replace on zonal/Rapid GCS buckets with a single XML API PUT.
 *
 * <p>Zonal buckets reject every JSON-API and gRPC single-shot write ({@code storage.create}, {@code
 * blobWriteSession}, {@code writer}: "This bucket requires appendable objects") and {@code
 * rewrite}, and an appendable upload aimed at the live target publishes whatever bytes it flushed
 * when the writer fails, whatever its {@code CloseAction}. Google documents that zonal buckets
 * accept XML API writes other than XML multipart uploads. A plain XML PUT carries {@code
 * x-goog-if-generation-match} and {@code x-goog-hash: crc32c=...}, returns the new generation in
 * {@code x-goog-generation}, and produces a finalized object.
 *
 * <p>Probed on a Rapid bucket in Oct 2026 (see {@code docs/docs/atomic_io_gcs_rapid.md}): a stale
 * generation gets 412, a CRC mismatch 400, a PUT whose body is cut off leaves the target unchanged,
 * readers see only the old or the new object, and of N racing writers exactly one commits. It
 * replaced stage-and-move (appendable upload to a temp object, then {@code moveBlob}), which took
 * three requests and a gRPC client.
 *
 * <p>java-storage has no XML upload surface, so the request is built on the client's own HTTP
 * transport and request initializer (credentials, timeouts, user agent) and host.
 */
class GCSXmlPutWriter {
  private final Storage storage;
  private final GCPProperties gcpProperties;
  private final HttpRequestFactory requests;
  private final String host;

  GCSXmlPutWriter(Storage storage, GCPProperties gcpProperties) {
    this.storage = storage;
    this.gcpProperties = gcpProperties;
    StorageOptions options = storage.getOptions();
    // A gRPC-transport client carries no HTTP transport; the default one still takes the
    // credentials and host from the client's options.
    HttpTransportOptions transport =
        options.getTransportOptions() instanceof HttpTransportOptions
            ? (HttpTransportOptions) options.getTransportOptions()
            : HttpTransportOptions.newBuilder().build();
    this.requests =
        transport
            .getHttpTransportFactory()
            .create()
            .createRequestFactory(transport.getHttpRequestInitializer(options));
    this.host = options.getHost();
  }

  /**
   * Writes {@code payload} to {@code target} in one request.
   *
   * @param pinnedSnapshot generation precondition: positive pins the live generation, {@code 0}
   *     requires that the object not exist, {@code null} means no precondition
   * @param crc32cB64 the payload's CRC32C, big-endian, base64; the server rejects a mismatch
   * @param gcsFileSystem the analytics-core file system for the returned InputFile, or null
   */
  InputFile writeAtomic(
      BlobId target,
      BlobId pinnedSnapshot,
      String crc32cB64,
      byte[] payload,
      AutoCloseable gcsFileSystem,
      MetricsContext metrics) {
    GenericUrl url = new GenericUrl(host);
    List<String> path = Lists.newArrayList("", target.getBucket());
    path.addAll(Splitter.on('/').splitToList(target.getName()));
    url.setPathParts(path);
    try {
      HttpRequest request =
          requests.buildPutRequest(url, new ByteArrayContent("application/octet-stream", payload));
      HttpHeaders headers = request.getHeaders();
      headers.set("x-goog-hash", "crc32c=" + crc32cB64);
      if (pinnedSnapshot != null) {
        headers.set("x-goog-if-generation-match", Long.toString(pinnedSnapshot.getGeneration()));
      }
      gcpProperties.userProject().ifPresent(project -> headers.set("x-goog-user-project", project));
      gcpProperties
          .encryptionKey()
          .ifPresent(
              key -> {
                headers.set("x-goog-encryption-algorithm", "AES256");
                headers.set("x-goog-encryption-key", key);
                headers.set("x-goog-encryption-key-sha256", sha256(key));
              });
      request.setThrowExceptionOnExecuteError(false);
      // No transport-level retries: the caller re-reads and retries on conflict and throttling,
      // and a blind retry of a PUT that landed would come back as a spurious 412.
      request.setNumberOfRetries(0);

      HttpResponse response = request.execute();
      try {
        int code = response.getStatusCode();
        if (code == 200) {
          long generation =
              Long.parseLong(response.getHeaders().getFirstHeaderStringValue("x-goog-generation"));
          metrics.counter(FileIOMetricsContext.WRITE_BYTES, Unit.BYTES).increment(payload.length);
          metrics.counter(FileIOMetricsContext.WRITE_OPERATIONS).increment();
          return new GCSInputFile(
              storage,
              gcsFileSystem,
              BlobId.of(target.getBucket(), target.getName(), generation),
              (long) payload.length,
              gcpProperties,
              metrics);
        }
        throw GCSOutputFile.translate(
            new StorageException(
                code,
                "XML PUT to " + target.toGsUtilUri() + " failed: " + response.parseAsString()));
      } finally {
        response.disconnect();
      }
    } catch (IOException e) {
      throw new UncheckedIOException("XML PUT to " + target.toGsUtilUri() + " failed", e);
    }
  }

  // x-goog-encryption-key-sha256 is the SHA-256 of the raw (base64-decoded) customer key.
  private static String sha256(String base64Key) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(Base64.getDecoder().decode(base64Key));
      return new String(Base64.getEncoder().encode(digest), StandardCharsets.US_ASCII);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }
}
