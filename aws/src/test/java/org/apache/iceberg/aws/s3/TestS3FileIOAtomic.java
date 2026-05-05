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
package org.apache.iceberg.aws.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import org.apache.commons.codec.digest.PureJavaCrc32C;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.relocated.com.google.common.io.CharStreams;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

@ExtendWith(TestS3FileIOAtomic.SuccessCleanupExtension.class)
public class TestS3FileIOAtomic {
  // private static final Logger LOG = LoggerFactory.getLogger(TestS3FileIOAtomic.class);
  // Standard bucket for CAS / if-match tests; override with S3_TEST_BUCKET.
  private static final String TEST_BUCKET =
      System.getenv().getOrDefault("S3_TEST_BUCKET", "lst-pbafvfgrapl");
  // S3 Express One Zone (directory) bucket required for APPEND tests; override with
  // S3_EXPRESS_TEST_BUCKET.
  private static final String EXPR_BUCKET =
      System.getenv().getOrDefault("S3_EXPRESS_TEST_BUCKET", "lst-pbafvfgrapl--usw2-az3--x-s3");

  private static S3Client s3;
  private static String uniqTestRun;
  private static String warehouseLocation;
  private static String warehousePath;

  @BeforeAll
  public static void initStorage() {
    // Skip test if AWS credentials are not available
    String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    Assumptions.assumeTrue(
        accessKey != null && secretKey != null,
        "AWS credentials not available - skipping S3 atomic tests");

    uniqTestRun = UUID.randomUUID().toString();
    System.err.println("TEST RUN: " + uniqTestRun);
    final AwsClientFactory clientFactory = AwsClientFactories.defaultFactory();
    s3 = clientFactory.s3();
    StaticClientFactory.client = s3;
  }

  @BeforeEach
  public void before(TestInfo info) {
    Assumptions.assumeTrue(s3 != null);
    final String testName = info.getTestMethod().orElseThrow(RuntimeException::new).getName();
    warehousePath = uniqTestRun + "/" + testName;
    warehouseLocation = "s3://" + TEST_BUCKET + "/" + warehousePath;
  }

  @AfterEach
  public void after() {
    // TODO
  }

  @Test
  public void testObjectPut() throws S3Exception {
    final String path = warehousePath + "/dingos";

    // write an object
    PutObjectRequest req1 = PutObjectRequest.builder().bucket(TEST_BUCKET).key(path).build();
    RequestBody body1 = RequestBody.fromBytes("ate my sandwich".getBytes(StandardCharsets.UTF_8));
    s3.putObject(req1, body1);
    PutObjectResponse resp1 = s3.putObject(req1, body1);

    // fail to overwrite it
    PutObjectRequest req2 =
        PutObjectRequest.builder().bucket(TEST_BUCKET).key(path).ifMatch("nope").build();
    RequestBody body2 = RequestBody.fromBytes("ate my tacos".getBytes(StandardCharsets.UTF_8));
    assertThatThrownBy(() -> s3.putObject(req2, body2))
        .isInstanceOf(S3Exception.class)
        .matches(e -> ((S3Exception) e).statusCode() == 412);

    // yup, still the same object
    HeadObjectRequest req3 = HeadObjectRequest.builder().bucket(TEST_BUCKET).key(path).build();
    assertThat(s3.headObject(req3)).extracting(HeadObjectResponse::eTag).isEqualTo(resp1.eTag());

    // overwrite w/ if-match
    PutObjectRequest req4 =
        PutObjectRequest.builder().bucket(TEST_BUCKET).key(path).ifMatch(resp1.eTag()).build();
    RequestBody body4 = RequestBody.fromBytes("ate my sushi".getBytes(StandardCharsets.UTF_8));
    PutObjectResponse resp4 = s3.putObject(req4, body4);

    // new object
    HeadObjectRequest req5 = HeadObjectRequest.builder().bucket(TEST_BUCKET).key(path).build();
    assertThat(s3.headObject(req5)).extracting(HeadObjectResponse::eTag).isEqualTo(resp4.eTag());
  }

  @Test
  public void testChecksum() throws S3Exception {
    final String path = warehousePath + "/wombats";

    final byte[] data = "cubed my pineapple".getBytes(StandardCharsets.UTF_8);

    final PureJavaCrc32C chk = new PureJavaCrc32C();
    chk.update(data, 0, data.length);
    String chkStr = Base64.getEncoder().encodeToString(Ints.toByteArray((int) chk.getValue()));

    PutObjectRequest req1 =
        PutObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(path)
            .checksumCRC32C(chkStr)
            .contentLength((long) data.length)
            .build();
    // RequestBody body1 = RequestBody.fromBytes(data);
    RequestBody body1 = RequestBody.fromInputStream(new ByteArrayInputStream(data), data.length);
    s3.putObject(req1, body1);
  }

  @Test
  public void testFileIOOverwrite() throws IOException, S3Exception {
    final String path = warehousePath + "/yaks";
    final String location = warehouseLocation + "/yaks";

    PutObjectRequest req1 = PutObjectRequest.builder().bucket(TEST_BUCKET).key(path).build();
    RequestBody body1 = RequestBody.fromBytes("shaved my kiwis".getBytes(StandardCharsets.UTF_8));
    s3.putObject(req1, body1);

    // let's see if this works
    S3FileIO fileIO = new S3FileIO(() -> s3);
    final InputFile inf = fileIO.newInputFile(location);
    try (InputStream i = inf.newStream()) {
      assertThat(CharStreams.toString(new InputStreamReader(i, StandardCharsets.UTF_8)))
          .isEqualTo("shaved my kiwis");
    }
    final AtomicOutputFile outf = fileIO.newOutputFile(inf);
    final byte[] replContent = "shaved my hamster".getBytes(StandardCharsets.UTF_8);
    final CAS chk =
        outf.prepare(() -> new ByteArrayInputStream(replContent), AtomicOutputFile.Strategy.CAS);

    InputFile replf = outf.writeAtomic(chk, () -> new ByteArrayInputStream(replContent));
    try (InputStream i = replf.newStream()) {
      assertThat(CharStreams.toString(new InputStreamReader(i, StandardCharsets.UTF_8)))
          .isEqualTo("shaved my hamster");
    }

    final AtomicOutputFile outfFail = fileIO.newOutputFile(inf);
    final byte[] failContent = "shaved your mom".getBytes(StandardCharsets.UTF_8);
    final CAS chkFail =
        outfFail.prepare(
            () -> new ByteArrayInputStream(failContent), AtomicOutputFile.Strategy.CAS);

    assertThatThrownBy(
            () -> outfFail.writeAtomic(chkFail, () -> new ByteArrayInputStream(failContent)))
        .isInstanceOf(SupportsAtomicOperations.CASException.class);
    try (InputStream i = replf.newStream()) {
      assertThat(CharStreams.toString(new InputStreamReader(i, StandardCharsets.UTF_8)))
          .isEqualTo("shaved my hamster");
    }
  }

  @Test
  public void testAppend() throws IOException, S3Exception {
    final String objName = "bananaslugs-" + uniqTestRun;
    final String path = "s3://" + EXPR_BUCKET + "/" + objName;

    // Match the checksum algorithm used by S3OutputFile.appendDestObj (CRC32C). The SDK >= 2.30
    // defaults to CRC32 when none is specified, which causes "Checksum Type mismatch" on append
    // against directory buckets, since append requires the same checksum type as the existing obj.
    PutObjectRequest req1 =
        PutObjectRequest.builder()
            .bucket(EXPR_BUCKET)
            .key(objName)
            .checksumAlgorithm(ChecksumAlgorithm.CRC32_C)
            .build();
    RequestBody body1 = RequestBody.fromBytes("shaved my kiwis".getBytes(StandardCharsets.UTF_8));
    PutObjectResponse resp1 = s3.putObject(req1, body1);

    // let's see if this works
    S3FileIO fileIO = new S3FileIO(() -> s3);
    final InputFile inf = fileIO.newInputFile(path);
    try (InputStream i = inf.newStream()) {
      assertThat(CharStreams.toString(new InputStreamReader(i, StandardCharsets.UTF_8)))
          .isEqualTo("shaved my kiwis");
    }

    final AtomicOutputFile outf = fileIO.newOutputFile(inf);
    final byte[] replContent = "shaved my hamster".getBytes(StandardCharsets.UTF_8);
    final CAS chk =
        outf.prepare(() -> new ByteArrayInputStream(replContent), AtomicOutputFile.Strategy.APPEND);
    InputFile replf = outf.writeAtomic(chk, () -> new ByteArrayInputStream(replContent));
    try (InputStream i = replf.newStream()) {
      assertThat(CharStreams.toString(new InputStreamReader(i, StandardCharsets.UTF_8)))
          .isEqualTo("shaved my kiwisshaved my hamster");
    }
  }

  @Test
  public void testAppendConditions() throws IOException, S3Exception {
    final String objName = "bananaslugs-cond-" + uniqTestRun;
    final String path = "s3://" + EXPR_BUCKET + "/" + objName;

    S3FileIO fileIO = new S3FileIO(() -> s3);
    fileIO.initialize(Maps.newHashMap());
    // Initial put uses CRC32C explicitly to match S3OutputFile.appendDestObj's hardcoded checksum
    // algorithm (see comment in testAppend).
    PutObjectRequest origReq =
        PutObjectRequest.builder()
            .bucket(EXPR_BUCKET)
            .key(objName)
            .checksumAlgorithm(ChecksumAlgorithm.CRC32_C)
            .build();
    s3.putObject(
        origReq, RequestBody.fromBytes("shaved my kiwis".getBytes(StandardCharsets.UTF_8)));
    final InputFile inf = fileIO.newInputFile(path);
    try (InputStream i = inf.newStream()) {
      assertThat(CharStreams.toString(new InputStreamReader(i, StandardCharsets.UTF_8)))
          .isEqualTo("shaved my kiwis");
    }

    final AtomicOutputFile app1 = fileIO.newOutputFile(inf);
    final byte[] replContent = "shaved my hamster".getBytes(StandardCharsets.UTF_8);
    final CAS chk1 =
        app1.prepare(() -> new ByteArrayInputStream(replContent), AtomicOutputFile.Strategy.APPEND);
    InputFile replf = app1.writeAtomic(chk1, () -> new ByteArrayInputStream(replContent));
    try (InputStream i = replf.newStream()) {
      assertThat(CharStreams.toString(new InputStreamReader(i, StandardCharsets.UTF_8)))
          .isEqualTo("shaved my kiwisshaved my hamster");
    }

    final AtomicOutputFile app3 = fileIO.newOutputFile(replf);
    final byte[] replContent3 = "shaved my yak".getBytes(StandardCharsets.UTF_8);
    final CAS chk3 =
        app3.prepare(
            () -> new ByteArrayInputStream(replContent3), AtomicOutputFile.Strategy.APPEND);
    InputFile replf3 = app3.writeAtomic(chk3, () -> new ByteArrayInputStream(replContent3));
    try (InputStream i = replf3.newStream()) {
      assertThat(CharStreams.toString(new InputStreamReader(i, StandardCharsets.UTF_8)))
          .isEqualTo("shaved my kiwisshaved my hamstershaved my yak");
    }

    final AtomicOutputFile app2 = fileIO.newOutputFile(inf);
    final CAS chk2 =
        app2.prepare(() -> new ByteArrayInputStream(replContent), AtomicOutputFile.Strategy.APPEND);
    assertThatThrownBy(() -> app2.writeAtomic(chk2, () -> new ByteArrayInputStream(replContent)))
        .isInstanceOf(SupportsAtomicOperations.AppendException.class);
  }

  /**
   * Walk the full atomic lifecycle on an S3 Express directory bucket — atomic create, append, CAS
   * replace at the same length, append — and at every stage verify a concurrent writer pinned to
   * the prior snapshot fails with the expected {@link SupportsAtomicOperations.CASException} or
   * {@link SupportsAtomicOperations.AppendException}. Each ghost writer prepares its checksum from
   * the same snapshot the winner uses, then commits *after* the winner has advanced the live object
   * — modeling the canonical "we both observed the same generation; only one of us may commit"
   * race.
   */
  @Test
  public void testCreateAppendCasAppendLifecycle() throws IOException, S3Exception {
    final String objName = "lifecycle-" + uniqTestRun;
    final String path = "s3://" + EXPR_BUCKET + "/" + objName;

    S3FileIO fileIO = new S3FileIO(() -> s3);
    fileIO.initialize(Maps.newHashMap());

    // ---- Stage 1: atomic create. Both writers pin "object does not exist". ----
    final InputFile snap0 = fileIO.newInputFile(path);
    assertThat(snap0.exists()).isFalse();

    final AtomicOutputFile winnerCreate = fileIO.newOutputFile(snap0);
    final byte[] payload1 = "shaved my kiwis".getBytes(StandardCharsets.UTF_8);
    final CAS tok1 =
        winnerCreate.prepare(
            () -> new ByteArrayInputStream(payload1), AtomicOutputFile.Strategy.CAS);

    final InputFile ghostSnap0 = fileIO.newInputFile(path);
    assertThat(ghostSnap0.exists()).isFalse();
    final AtomicOutputFile ghostCreate = fileIO.newOutputFile(ghostSnap0);
    final byte[] ghostPayload1 = "ghost create".getBytes(StandardCharsets.UTF_8);
    final CAS ghostTok1 =
        ghostCreate.prepare(
            () -> new ByteArrayInputStream(ghostPayload1), AtomicOutputFile.Strategy.CAS);

    InputFile after1 = winnerCreate.writeAtomic(tok1, () -> new ByteArrayInputStream(payload1));
    assertThat(readAll(after1)).isEqualTo("shaved my kiwis");

    // Stale create: object now exists, ifNoneMatch=* must reject the write.
    assertThatThrownBy(
            () -> ghostCreate.writeAtomic(ghostTok1, () -> new ByteArrayInputStream(ghostPayload1)))
        .isInstanceOf(SupportsAtomicOperations.CASException.class);
    assertThat(readAll(fileIO.newInputFile(path))).isEqualTo("shaved my kiwis");

    // ---- Stage 2: APPEND on the freshly-created object. ----
    // after1's etag is the winner's create response; the ghost pins the same snapshot the winner
    // is about to commit against.
    final AtomicOutputFile winnerAppend1 = fileIO.newOutputFile(after1);
    final byte[] payload2 = "shaved my hamster".getBytes(StandardCharsets.UTF_8);
    final CAS tok2 =
        winnerAppend1.prepare(
            () -> new ByteArrayInputStream(payload2), AtomicOutputFile.Strategy.APPEND);

    final AtomicOutputFile ghostAppend1 = fileIO.newOutputFile(after1);
    final byte[] ghostPayload2 = "ghost append".getBytes(StandardCharsets.UTF_8);
    final CAS ghostTok2 =
        ghostAppend1.prepare(
            () -> new ByteArrayInputStream(ghostPayload2), AtomicOutputFile.Strategy.APPEND);

    InputFile after2 = winnerAppend1.writeAtomic(tok2, () -> new ByteArrayInputStream(payload2));
    assertThat(readAll(after2)).isEqualTo("shaved my kiwisshaved my hamster");

    // Stale append: the etag the ghost pinned is no longer live; ifMatch must fail.
    assertThatThrownBy(
            () ->
                ghostAppend1.writeAtomic(ghostTok2, () -> new ByteArrayInputStream(ghostPayload2)))
        .isInstanceOf(SupportsAtomicOperations.AppendException.class);
    assertThat(readAll(fileIO.newInputFile(path))).isEqualTo("shaved my kiwisshaved my hamster");

    // ---- Stage 3: CAS replace at the *same* length as the appended object. ----
    final long appendedLen = "shaved my kiwisshaved my hamster".length();
    final byte[] payload3 = padTo("shaved my pickles", (int) appendedLen);
    assertThat(payload3.length).isEqualTo((int) appendedLen);
    final AtomicOutputFile winnerCas = fileIO.newOutputFile(after2);
    final CAS tok3 =
        winnerCas.prepare(() -> new ByteArrayInputStream(payload3), AtomicOutputFile.Strategy.CAS);

    final byte[] ghostPayload3 = padTo("ghost replace", (int) appendedLen);
    final AtomicOutputFile ghostCas = fileIO.newOutputFile(after2);
    final CAS ghostTok3 =
        ghostCas.prepare(
            () -> new ByteArrayInputStream(ghostPayload3), AtomicOutputFile.Strategy.CAS);

    InputFile after3 = winnerCas.writeAtomic(tok3, () -> new ByteArrayInputStream(payload3));
    assertThat(readAllBytes(after3)).isEqualTo(payload3);

    // Stale CAS: same length but stale etag must fail.
    assertThatThrownBy(
            () -> ghostCas.writeAtomic(ghostTok3, () -> new ByteArrayInputStream(ghostPayload3)))
        .isInstanceOf(SupportsAtomicOperations.CASException.class);
    assertThat(readAllBytes(fileIO.newInputFile(path))).isEqualTo(payload3);

    // ---- Stage 4: APPEND after CAS replace. ----
    final AtomicOutputFile winnerAppend2 = fileIO.newOutputFile(after3);
    final byte[] payload4 = "shaved my yak".getBytes(StandardCharsets.UTF_8);
    final CAS tok4 =
        winnerAppend2.prepare(
            () -> new ByteArrayInputStream(payload4), AtomicOutputFile.Strategy.APPEND);

    final AtomicOutputFile ghostAppend2 = fileIO.newOutputFile(after3);
    final byte[] ghostPayload4 = "ghost yak".getBytes(StandardCharsets.UTF_8);
    final CAS ghostTok4 =
        ghostAppend2.prepare(
            () -> new ByteArrayInputStream(ghostPayload4), AtomicOutputFile.Strategy.APPEND);

    InputFile after4 = winnerAppend2.writeAtomic(tok4, () -> new ByteArrayInputStream(payload4));

    final byte[] expectedFinal = new byte[payload3.length + payload4.length];
    System.arraycopy(payload3, 0, expectedFinal, 0, payload3.length);
    System.arraycopy(payload4, 0, expectedFinal, payload3.length, payload4.length);
    assertThat(readAllBytes(after4)).isEqualTo(expectedFinal);

    // Stale append after CAS: ghost still has after3's etag, which CAS replaced and the winner
    // has now appended past. ifMatch must fail.
    assertThatThrownBy(
            () ->
                ghostAppend2.writeAtomic(ghostTok4, () -> new ByteArrayInputStream(ghostPayload4)))
        .isInstanceOf(SupportsAtomicOperations.AppendException.class);
    assertThat(readAllBytes(fileIO.newInputFile(path))).isEqualTo(expectedFinal);
  }

  private static byte[] padTo(String s, int length) {
    byte[] src = s.getBytes(StandardCharsets.UTF_8);
    if (src.length >= length) {
      byte[] truncated = new byte[length];
      System.arraycopy(src, 0, truncated, 0, length);
      return truncated;
    }
    byte[] out = new byte[length];
    System.arraycopy(src, 0, out, 0, src.length);
    for (int i = src.length; i < length; i++) {
      out[i] = '.';
    }
    return out;
  }

  private static String readAll(InputFile in) throws IOException {
    try (InputStream s = in.newStream()) {
      return CharStreams.toString(new InputStreamReader(s, StandardCharsets.UTF_8));
    }
  }

  private static byte[] readAllBytes(InputFile in) throws IOException {
    try (InputStream s = in.newStream()) {
      return ByteStreams.toByteArray(s);
    }
  }

  static class SuccessCleanupExtension implements TestWatcher {
    @Override
    public void testSuccessful(ExtensionContext ctxt) {
      cleanupWarehouseLocation();
    }
  }

  static void cleanupWarehouseLocation() {
    // use FileIO
  }
}
