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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.BaseServiceException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobAppendableUploadConfig.CloseAction;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Flow;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Canaries for the zonal/Rapid bucket behaviour that {@link GCSXmlPutWriter} is built on. Run
 * against a real Rapid bucket: set {@code RAPID_BUCKET} (and use ADC or {@code
 * GOOGLE_APPLICATION_CREDENTIALS}); skipped otherwise.
 *
 * <ul>
 *   <li><b>Gap canaries</b> assert that zonal buckets still reject the JSON-API single-request
 *       create and the resumable upload, and the XML API's resumable upload. When one of the first
 *       two fails, the standard-bucket paths may serve zonal buckets too ({@code storage.create}
 *       for CAS, {@link GCSOutputStream} for regular writes); when the third does, regular writes
 *       could stream over HTTP instead of a gRPC appendable upload.
 *   <li>The <b>hazard canary</b> asserts that an appendable upload aimed at the live object still
 *       publishes partial bytes when the writer fails. That is why nothing writes appendable
 *       objects to a catalog's live file.
 *   <li><b>Positive canaries</b> assert the XML PUT guarantees the writer relies on: generation
 *       preconditions, CRC32C validation, and that a PUT cut off mid-body changes nothing. If one
 *       fails, CAS on Rapid buckets is unsafe until the cause is understood.
 * </ul>
 *
 * <p>All three groups were last verified on 2026-10-14 (google-cloud-storage 2.67.0).
 */
public class GcsRapidCanaries {
  private static final String RAPID_BUCKET_ENV = "RAPID_BUCKET";

  private static String bucket;
  private static Storage json;
  private static Storage grpc;
  private static GoogleCredentials credentials;
  private static final HttpClient HTTP =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
  private static final List<BlobId> CREATED = Lists.newArrayList();

  @BeforeAll
  static void setUp() throws IOException {
    bucket = System.getenv(RAPID_BUCKET_ENV);
    if (bucket == null || bucket.isEmpty()) {
      return;
    }
    credentials =
        GoogleCredentials.getApplicationDefault()
            .createScoped("https://www.googleapis.com/auth/devstorage.read_write");
    json = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    grpc =
        StorageOptions.grpc()
            .setCredentials(credentials)
            .setEnableGrpcClientMetrics(false)
            .build()
            .getService();
  }

  @AfterAll
  static void tearDown() throws Exception {
    for (BlobId id : CREATED) {
      try {
        json.delete(BlobId.of(id.getBucket(), id.getName()));
      } catch (RuntimeException e) {
        // best-effort cleanup
      }
    }
    if (grpc != null) {
      grpc.close();
    }
  }

  @BeforeEach
  void requireRapidBucket() {
    Assumptions.assumeTrue(
        bucket != null && !bucket.isEmpty(),
        "Real Rapid GCS bucket not configured; set " + RAPID_BUCKET_ENV);
  }

  // ─── gap canaries ───────────────────────────────────────────────────────────────────────────

  @Test
  void zonalRejectsJsonCreate() {
    BlobId id = object("json-create");
    long generation = xmlPut(id, bytes("v1"), 0).generation;
    assertThatThrownBy(
            () ->
                json.create(
                    BlobInfo.newBuilder(id).build(),
                    bytes("v2"),
                    BlobTargetOption.generationMatch(generation)))
        .as(
            "The zonal bucket accepted storage.create. If the object is finalized and the "
                + "generation precondition holds, GCSOutputFile.createAtomic can serve zonal "
                + "buckets and GCSXmlPutWriter can go.")
        .isInstanceOf(BaseServiceException.class)
        .satisfies(e -> assertThat(((BaseServiceException) e).getCode()).isEqualTo(400));
  }

  @Test
  void zonalRejectsResumableUpload() {
    BlobId id = object("resumable");
    long generation = xmlPut(id, bytes("v1"), 0).generation;
    assertThatThrownBy(
            () -> {
              BlobWriteSession session =
                  json.blobWriteSession(
                      BlobInfo.newBuilder(id).build(), BlobWriteOption.generationMatch(generation));
              try (WritableByteChannel channel = session.open()) {
                channel.write(ByteBuffer.wrap(bytes("v2")));
              }
              session.getResult().get();
            })
        .as("The zonal bucket accepted a resumable upload; see zonalRejectsJsonCreate")
        .hasRootCauseInstanceOf(com.google.api.client.http.HttpResponseException.class);
    assertThat(read(id)).isEqualTo("v1");
  }

  @Test
  void zonalRejectsXmlResumableUpload() throws Exception {
    BlobId id = object("xml-resumable");
    HttpResponse<String> response =
        HTTP.send(
            put(id, 0)
                .header("x-goog-resumable", "start")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode())
        .as(
            "The zonal bucket accepted an XML resumable upload. It would stream regular writes "
                + "over HTTP and publish them only when complete; consider it in place of the "
                + "gRPC appendable upload in GCSAppendableOutputStream.")
        .isEqualTo(400);
    assertThat(response.body()).contains("ZonalBucketIncompatibleFeature");
  }

  // ─── hazard canary ──────────────────────────────────────────────────────────────────────────

  @Test
  void abandonedAppendableUploadPublishesPartialBytes() throws Exception {
    for (CloseAction action : CloseAction.values()) {
      BlobId id = object("appendable-" + action);
      long generation = xmlPut(id, bytes("complete-old-version"), 0).generation;
      byte[] payload = new byte[1 << 20];
      Arrays.fill(payload, (byte) 'n');
      BlobAppendableUpload upload =
          grpc.blobAppendableUpload(
              BlobInfo.newBuilder(id).build(),
              BlobAppendableUploadConfig.of().withCloseAction(action),
              BlobWriteOption.generationMatch(generation));
      try (BlobAppendableUpload.AppendableUploadWriteableByteChannel channel = upload.open()) {
        channel.write(ByteBuffer.wrap(payload, 0, payload.length / 2));
        channel.flush();
        throw new IllegalStateException("writer fails after half the payload");
      } catch (IllegalStateException expected) {
        // the writer's failure path: try-with-resources closes the channel
      }
      Blob now = json.get(id);
      assertThat(now.getGeneration())
          .as(
              "CloseAction.%s: an abandoned appendable upload no longer replaced the live "
                  + "object. If appendable uploads became all-or-nothing, re-measure them "
                  + "against GCSXmlPutWriter.",
              action)
          .isNotEqualTo(generation);
      // The old complete version is gone and what replaced it is not the full payload. (The
      // reported size of the unfinalized object lags the flushed bytes, so only the bound is
      // stable: 0 right after close, half the payload shortly after.)
      assertThat(now.getSize()).isLessThan((long) payload.length);
    }
  }

  // ─── positive canaries ──────────────────────────────────────────────────────────────────────

  @Test
  void xmlPutEnforcesGenerationPreconditions() {
    BlobId id = object("xml-generation");
    Put v1 = xmlPut(id, bytes("v1"), 0);
    assertThat(v1.status).isEqualTo(200);
    assertThat(xmlPut(id, bytes("again"), 0).status)
        .as("if-generation-match: 0 on an existing object")
        .isEqualTo(412);
    Put v2 = xmlPut(id, bytes("v2"), v1.generation);
    assertThat(v2.status).isEqualTo(200);
    assertThat(v2.generation).isNotEqualTo(v1.generation);
    assertThat(xmlPut(id, bytes("v3"), v1.generation).status)
        .as("if-generation-match on a stale generation")
        .isEqualTo(412);
    assertThat(read(id)).isEqualTo("v2");
  }

  @Test
  void xmlPutValidatesCrc32c() throws Exception {
    BlobId id = object("xml-crc32c");
    long generation = xmlPut(id, bytes("v1"), 0).generation;
    HttpResponse<String> response =
        HTTP.send(
            put(id, generation)
                .header("x-goog-hash", "crc32c=AAAAAA==")
                .PUT(HttpRequest.BodyPublishers.ofByteArray(bytes("v2")))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as("PUT with a wrong x-goog-hash crc32c").isEqualTo(400);
    assertThat(read(id)).isEqualTo("v1");
  }

  @Test
  void xmlPutCutOffMidBodyChangesNothing() throws Exception {
    BlobId id = object("xml-cut-off");
    long generation = xmlPut(id, bytes("v1"), 0).generation;
    // Declares 1 MiB, sends 256 KiB, then the client fails.
    Flow.Publisher<ByteBuffer> dies =
        subscriber ->
            subscriber.onSubscribe(
                new Flow.Subscription() {
                  private boolean sent = false;

                  @Override
                  public void request(long n) {
                    if (!sent) {
                      sent = true;
                      subscriber.onNext(ByteBuffer.wrap(new byte[256 << 10]));
                      subscriber.onError(new IOException("writer died mid-body"));
                    }
                  }

                  @Override
                  public void cancel() {}
                });
    assertThatThrownBy(
            () ->
                HTTP.send(
                    put(id, generation)
                        .PUT(HttpRequest.BodyPublishers.fromPublisher(dies, 1 << 20))
                        .build(),
                    HttpResponse.BodyHandlers.ofString()))
        .isInstanceOf(IOException.class);
    Blob now = json.get(id);
    assertThat(now.getGeneration()).as("generation after a cut-off PUT").isEqualTo(generation);
    assertThat(read(id)).isEqualTo("v1");
  }

  // ─── helpers ────────────────────────────────────────────────────────────────────────────────

  private static class Put {
    private final int status;
    private final long generation;

    Put(int status, long generation) {
      this.status = status;
      this.generation = generation;
    }
  }

  private static Put xmlPut(BlobId id, byte[] body, long ifGenerationMatch) {
    try {
      HttpResponse<String> response =
          HTTP.send(
              put(id, ifGenerationMatch).PUT(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
              HttpResponse.BodyHandlers.ofString());
      return new Put(
          response.statusCode(),
          Long.parseLong(response.headers().firstValue("x-goog-generation").orElse("-1")));
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static HttpRequest.Builder put(BlobId id, long ifGenerationMatch) {
    try {
      credentials.refreshIfExpired();
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    return HttpRequest.newBuilder(
            URI.create("https://storage.googleapis.com/" + id.getBucket() + "/" + id.getName()))
        .header("Authorization", "Bearer " + credentials.getAccessToken().getTokenValue())
        .header("x-goog-if-generation-match", Long.toString(ifGenerationMatch));
  }

  private static BlobId object(String slug) {
    BlobId id = BlobId.of(bucket, "canaries/" + slug + "-" + UUID.randomUUID());
    CREATED.add(id);
    return id;
  }

  private static String read(BlobId id) {
    return new String(json.readAllBytes(id), StandardCharsets.UTF_8);
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }
}
