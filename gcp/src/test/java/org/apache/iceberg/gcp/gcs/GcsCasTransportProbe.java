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

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobAppendableUploadConfig.CloseAction;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Stage 0 SDK survey for GCS CAS-replace.
 *
 * <p>The current production path uses {@code storage.blobWriteSession(...)} on an HTTP-transport
 * {@link Storage} client. That path fails on zonal Rapid buckets with HTTP 400 ("Zonal buckets are
 * incompatible with resumable upload") because the default session config selects resumable upload,
 * which zonal buckets reject.
 *
 * <p>This probe surveys whether the SDK exposes a single CAS-replace API that works against both
 * standard and zonal buckets, so we can avoid forking the production write path on bucket type.
 *
 * <p>Each cell is a (transport, write-API, bucket) combination. Per cell the probe runs:
 *
 * <ol>
 *   <li>Create with {@code generationMatch(0)} (object must not exist) and {@code crc32cMatch}.
 *   <li>Replace with {@code generationMatch(g1)} and {@code crc32cMatch}; capture g2.
 *   <li>Stale-gen retry with {@code generationMatch(g1)}; expect rejection (HTTP 412).
 *   <li>CRC32C corruption: write payload P with the CRC of payload P'; expect rejection.
 * </ol>
 *
 * <p>Each cell prints a {@code [probe.<cell>]} line with the four outcomes; a summary table is
 * printed in {@link #report()} after all parameterized invocations finish.
 *
 * <p><b>Run:</b>
 *
 * <pre>{@code
 * GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json \
 * STANDARD_BUCKET=lst-consistency RAPID_BUCKET=lstx-consistency \
 *   ./gradlew :iceberg-gcp:test \
 *   --tests org.apache.iceberg.gcp.gcs.GcsCasTransportProbe \
 *   -x generateGitProperties --info
 * }</pre>
 *
 * Cells whose bucket env var is unset are skipped.
 */
public class GcsCasTransportProbe {

  private static final String STANDARD_BUCKET_ENV = "STANDARD_BUCKET";
  private static final String RAPID_BUCKET_ENV = "RAPID_BUCKET";
  private static final String CREDS_ENV = "GOOGLE_APPLICATION_CREDENTIALS";
  private static final String SUITE_PREFIX = "iceberg-cas-probe/" + UUID.randomUUID() + "/";

  private static String standardBucket;
  private static String zonalBucket;
  private static Storage httpClient;
  private static Storage grpcClient;
  private static final Map<String, Outcome> RESULTS = new LinkedHashMap<>();

  private final List<BlobId> created = new ArrayList<>();

  // ─── lifecycle ────────────────────────────────────────────────────────────────────────────

  @BeforeAll
  static void setupClass() throws IOException {
    standardBucket = System.getenv(STANDARD_BUCKET_ENV);
    zonalBucket = System.getenv(RAPID_BUCKET_ENV);
    if ((standardBucket == null || standardBucket.isEmpty())
        && (zonalBucket == null || zonalBucket.isEmpty())) {
      System.out.println(
          "[probe.setup] no buckets configured; set "
              + STANDARD_BUCKET_ENV
              + " and/or "
              + RAPID_BUCKET_ENV
              + " to run");
      return;
    }

    Credentials creds = loadCredentials();
    StorageOptions.Builder httpBuilder = StorageOptions.http();
    StorageOptions.Builder grpcBuilder = StorageOptions.grpc();
    if (creds != null) {
      httpBuilder.setCredentials(creds);
      grpcBuilder.setCredentials(creds);
    }
    String project = System.getenv("GOOGLE_CLOUD_PROJECT");
    if (project != null && !project.isEmpty()) {
      httpBuilder.setProjectId(project);
      grpcBuilder.setProjectId(project);
    }
    httpClient = httpBuilder.build().getService();
    grpcClient = grpcBuilder.build().getService();
    System.out.println(
        "[probe.setup] standard="
            + standardBucket
            + " zonal="
            + zonalBucket
            + " httpClient="
            + httpClient.getClass().getSimpleName()
            + " grpcClient="
            + grpcClient.getClass().getSimpleName()
            + " prefix="
            + SUITE_PREFIX);
  }

  @AfterAll
  static void teardownClass() throws Exception {
    if (httpClient != null) {
      httpClient.close();
    }
    if (grpcClient != null) {
      grpcClient.close();
    }
    report();
  }

  @AfterEach
  void cleanupObjects() {
    if (httpClient == null) {
      return;
    }
    for (BlobId id : created) {
      try {
        httpClient.delete(id);
      } catch (RuntimeException ignored) {
        // best-effort
      }
    }
    created.clear();
  }

  // ─── matrix ───────────────────────────────────────────────────────────────────────────────

  enum Transport {
    HTTP,
    GRPC
  }

  enum BucketKind {
    STANDARD,
    ZONAL
  }

  static Stream<Arguments> cells() {
    List<Arguments> rows = new ArrayList<>();
    Transport[] transports = {Transport.HTTP, Transport.GRPC};
    BucketKind[] buckets = {BucketKind.STANDARD, BucketKind.ZONAL};
    Writer[] writers = {
      new BlobWriteSessionWriter(), new BlobAppendableUploadWriter(), new StorageCreateWriter()
    };
    for (Writer w : writers) {
      for (Transport t : transports) {
        for (BucketKind b : buckets) {
          rows.add(Arguments.of(t, w, b, label(t, w, b)));
        }
      }
    }
    return rows.stream();
  }

  private static String label(Transport t, Writer w, BucketKind b) {
    return String.format(
        Locale.ROOT,
        "%s.%s.%s",
        w.shortName(),
        t.name().toLowerCase(Locale.ROOT),
        b.name().toLowerCase(Locale.ROOT));
  }

  @ParameterizedTest(name = "{3}")
  @MethodSource("cells")
  void probe(Transport transport, Writer writer, BucketKind bucketKind, String label) {
    String bucket = (bucketKind == BucketKind.STANDARD) ? standardBucket : zonalBucket;
    Assumptions.assumeTrue(
        bucket != null && !bucket.isEmpty(), "bucket env not set for " + bucketKind);
    Storage storage = (transport == Transport.HTTP) ? httpClient : grpcClient;
    Assumptions.assumeTrue(storage != null, "no storage client for " + transport);

    Outcome out = new Outcome();
    BlobId target = freshKey(bucket, label);

    byte[] payload1 = randomBytes(4096, 0xC0FFEE);
    byte[] crc1 = crc32c(payload1);

    // (1) create with generationMatch(0)
    long gen1 = -1;
    try {
      gen1 = writer.write(storage, target, payload1, 0L, crc1);
      out.create = "ok gen=" + gen1;
    } catch (Throwable t) {
      out.create = "FAIL " + classify(t);
      finish(label, out);
      return;
    }

    // (2) replace with generationMatch(gen1)
    byte[] payload2 = randomBytes(4096, 0xBADCAFE);
    byte[] crc2 = crc32c(payload2);
    long gen2 = -1;
    try {
      gen2 = writer.write(storage, target, payload2, gen1, crc2);
      out.replace = "ok gen=" + gen2;
    } catch (Throwable t) {
      out.replace = "FAIL " + classify(t);
      finish(label, out);
      return;
    }

    // (3) stale-gen retry against the now-replaced object — should reject with 412
    byte[] payload3 = randomBytes(4096, 0xDEADBEEF);
    byte[] crc3 = crc32c(payload3);
    try {
      writer.write(storage, target, payload3, gen1, crc3);
      out.staleGen = "BUG accepted stale gen";
    } catch (Throwable t) {
      out.staleGen = "rejected " + classify(t);
    }

    // (4) CRC32C corruption — claim crc(payload2) for payload3, write payload3
    try {
      writer.write(storage, target, payload3, gen2, crc2);
      out.crc = "BUG accepted bad CRC";
    } catch (Throwable t) {
      out.crc = "rejected " + classify(t);
    }

    finish(label, out);
  }

  private void finish(String label, Outcome out) {
    System.out.println("[probe." + label + "] " + out.summary());
    RESULTS.put(label, out);
  }

  static void report() {
    if (RESULTS.isEmpty()) {
      return;
    }
    System.out.println();
    System.out.println("=== GcsCasTransportProbe summary ===");
    System.out.printf(
        "%-40s %-22s %-22s %-26s %-26s%n",
        "cell", "create", "replace", "stale-gen", "crc-mismatch");
    System.out.printf(
        "%-40s %-22s %-22s %-26s %-26s%n",
        "----", "------", "-------", "---------", "------------");
    for (Map.Entry<String, Outcome> e : RESULTS.entrySet()) {
      Outcome o = e.getValue();
      System.out.printf(
          "%-40s %-22s %-22s %-26s %-26s%n",
          e.getKey(),
          truncate(o.create, 22),
          truncate(o.replace, 22),
          truncate(o.staleGen, 26),
          truncate(o.crc, 26));
    }
  }

  private static String truncate(String s, int n) {
    if (s == null) {
      return "-";
    }
    return s.length() <= n ? s : s.substring(0, n - 1) + "…";
  }

  // ─── writers ──────────────────────────────────────────────────────────────────────────────

  interface Writer {
    /**
     * Perform a CAS write of {@code payload} to {@code target}, gated on {@code generationMatch} of
     * {@code expectedGen} (0 = does-not-exist) and {@code crc32cMatch} of {@code expectedCrc32c}.
     * Returns the new object's generation on success.
     */
    long write(
        Storage storage, BlobId target, byte[] payload, long expectedGen, byte[] expectedCrc32c)
        throws Exception;

    String shortName();
  }

  /** {@code storage.blobWriteSession(info, options)} — current production path. */
  static final class BlobWriteSessionWriter implements Writer {
    @Override
    public long write(Storage storage, BlobId target, byte[] payload, long expectedGen, byte[] crc)
        throws Exception {
      BlobInfo info = BlobInfo.newBuilder(target).setCrc32c(b64(crc)).build();
      BlobWriteSession sess =
          storage.blobWriteSession(
              info, BlobWriteOption.generationMatch(expectedGen), BlobWriteOption.crc32cMatch());
      try (WritableByteChannel ch = sess.open()) {
        try (OutputStream os = Channels.newOutputStream(ch)) {
          os.write(payload);
        }
      }
      return sess.getResult().get().getGeneration();
    }

    @Override
    public String shortName() {
      return "session";
    }
  }

  /** {@code storage.blobAppendableUpload(info, config, options)} — gRPC-only, bidi/append API. */
  static final class BlobAppendableUploadWriter implements Writer {
    @Override
    public long write(Storage storage, BlobId target, byte[] payload, long expectedGen, byte[] crc)
        throws Exception {
      BlobInfo info = BlobInfo.newBuilder(target).setCrc32c(b64(crc)).build();
      BlobAppendableUpload up =
          storage.blobAppendableUpload(
              info,
              BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING),
              BlobWriteOption.generationMatch(expectedGen),
              BlobWriteOption.crc32cMatch());
      try (BlobAppendableUpload.AppendableUploadWriteableByteChannel ch = up.open()) {
        ch.write(ByteBuffer.wrap(payload));
        ch.finalizeAndClose();
      }
      return up.getResult().get().getGeneration();
    }

    @Override
    public String shortName() {
      return "appendable";
    }
  }

  /** {@code storage.create(info, bytes, BlobTargetOption...)} — single-shot insert. */
  static final class StorageCreateWriter implements Writer {
    @Override
    public long write(Storage storage, BlobId target, byte[] payload, long expectedGen, byte[] crc)
        throws Exception {
      // For storage.create(), the expected CRC32C is carried on BlobInfo and validated server-side
      // without an explicit option flag. BlobTargetOption only exposes generationMatch here.
      BlobInfo info = BlobInfo.newBuilder(target).setCrc32c(b64(crc)).build();
      return storage
          .create(info, payload, BlobTargetOption.generationMatch(expectedGen))
          .getGeneration();
    }

    @Override
    public String shortName() {
      return "create";
    }
  }

  // ─── helpers ──────────────────────────────────────────────────────────────────────────────

  private BlobId freshKey(String bucket, String label) {
    BlobId id = BlobId.of(bucket, SUITE_PREFIX + label + "/" + UUID.randomUUID() + ".bin");
    created.add(BlobId.of(bucket, id.getName()));
    return id;
  }

  private static byte[] randomBytes(int n, long seed) {
    Random r = new Random(seed);
    byte[] b = new byte[n];
    r.nextBytes(b);
    return b;
  }

  private static byte[] crc32c(byte[] data) {
    int crc = Hashing.crc32c().hashBytes(data).asInt();
    return new byte[] {(byte) (crc >>> 24), (byte) (crc >>> 16), (byte) (crc >>> 8), (byte) crc};
  }

  private static String b64(byte[] data) {
    return BaseEncoding.base64().encode(data);
  }

  private static String classify(Throwable t) {
    Throwable cur = t;
    while (cur != null) {
      if (cur instanceof StorageException) {
        StorageException se = (StorageException) cur;
        String reason = se.getReason() != null ? se.getReason() : "";
        return "StorageException(code="
            + se.getCode()
            + (reason.isEmpty() ? "" : ", reason=" + reason)
            + ", msg="
            + truncateMsg(se.getMessage())
            + ")";
      }
      cur = cur.getCause();
    }
    return t.getClass().getSimpleName() + ": " + truncateMsg(t.getMessage());
  }

  private static String truncateMsg(String m) {
    if (m == null) {
      return "<null>";
    }
    String oneLine = m.replace('\n', ' ').replace('\r', ' ');
    return oneLine.length() <= 80 ? oneLine : oneLine.substring(0, 79) + "…";
  }

  private static Credentials loadCredentials() throws IOException {
    String credPath = System.getenv(CREDS_ENV);
    if (credPath != null && !credPath.isEmpty()) {
      Path p = Paths.get(credPath);
      if (Files.exists(p)) {
        try (FileInputStream in = new FileInputStream(p.toFile())) {
          return GoogleCredentials.fromStream(in);
        }
      }
    }
    try {
      return GoogleCredentials.getApplicationDefault();
    } catch (IOException e) {
      System.out.println("[probe.setup] no application-default credentials: " + e.getMessage());
      return null;
    }
  }

  // ─── per-cell outcome ─────────────────────────────────────────────────────────────────────

  static final class Outcome {
    String create = "-";
    String replace = "-";
    String staleGen = "-";
    String crc = "-";

    String summary() {
      return "create="
          + create
          + " | replace="
          + replace
          + " | stale="
          + staleGen
          + " | crc="
          + crc;
    }
  }
}
