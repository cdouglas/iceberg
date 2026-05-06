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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import org.apache.commons.codec.digest.PureJavaCrc32C;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.io.SupportsAtomicOperationsContractTest;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Standard-bucket S3 implementation of the {@link SupportsAtomicOperationsContractTest}. APPEND is
 * not supported on standard buckets; APPEND-gated tests skip via the assumption baked into the
 * contract.
 */
public class S3StandardFileIOAtomicTest extends SupportsAtomicOperationsContractTest {

  private static final String TEST_BUCKET =
      System.getenv().getOrDefault("S3_TEST_BUCKET", "lst-pbafvfgrapl");
  private static String runId;
  private static S3Client s3;

  @BeforeAll
  static void initStorage() {
    String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    if (accessKey != null && secretKey != null) {
      runId = UUID.randomUUID().toString();
      s3 = AwsClientFactories.defaultFactory().s3();
    }
  }

  @BeforeEach
  void requireCredentials() {
    Assumptions.assumeTrue(s3 != null, "AWS credentials not available");
  }

  @Override
  protected boolean supportsAppend() {
    // S3FileIO.supportsAppend() reports true (the SDK supports writeOffsetBytes); only directory
    // buckets accept it server-side, so override to false for this standard-bucket test.
    return false;
  }

  @Override
  protected SupportsAtomicOperations newFileIO() {
    S3FileIO io = new S3FileIO(() -> s3);
    io.initialize(Maps.newHashMap());
    return io;
  }

  @Override
  protected String randomLocation(String slug) {
    return String.format("s3://%s/%s/%s-%s", TEST_BUCKET, runId, slug, UUID.randomUUID());
  }

  // ─── Raw S3 SDK behavior smoke tests (not part of the FileIO contract) ──────────────────────

  /**
   * Sanity-check the underlying S3 SDK primitives the {@link S3FileIO} CAS path depends on:
   * If-Match preconditions on PutObject return 412 on mismatch and succeed on match.
   */
  @Test
  void rawS3PutObjectIfMatchPrecondition() {
    String key = String.format("%s/raw-if-match/%s", runId, UUID.randomUUID());

    PutObjectRequest req1 = PutObjectRequest.builder().bucket(TEST_BUCKET).key(key).build();
    RequestBody body1 = RequestBody.fromBytes("ate my sandwich".getBytes(StandardCharsets.UTF_8));
    PutObjectResponse resp1 = s3.putObject(req1, body1);

    // Stale If-Match must fail with 412.
    PutObjectRequest req2 =
        PutObjectRequest.builder().bucket(TEST_BUCKET).key(key).ifMatch("nope").build();
    RequestBody body2 = RequestBody.fromBytes("ate my tacos".getBytes(StandardCharsets.UTF_8));
    assertThatThrownBy(() -> s3.putObject(req2, body2))
        .isInstanceOf(S3Exception.class)
        .matches(e -> ((S3Exception) e).statusCode() == 412);

    // Object is unchanged.
    HeadObjectRequest head = HeadObjectRequest.builder().bucket(TEST_BUCKET).key(key).build();
    assertThat(s3.headObject(head)).extracting(HeadObjectResponse::eTag).isEqualTo(resp1.eTag());

    // Live If-Match succeeds.
    PutObjectRequest req3 =
        PutObjectRequest.builder().bucket(TEST_BUCKET).key(key).ifMatch(resp1.eTag()).build();
    RequestBody body3 = RequestBody.fromBytes("ate my sushi".getBytes(StandardCharsets.UTF_8));
    PutObjectResponse resp3 = s3.putObject(req3, body3);
    assertThat(s3.headObject(head)).extracting(HeadObjectResponse::eTag).isEqualTo(resp3.eTag());
  }

  /**
   * Sanity-check that the SDK accepts a CRC32C content checksum on PutObject — the FileIO CAS path
   * pins this checksum in {@code S3OutputFile.replaceDestObj} for end-to-end integrity.
   */
  @Test
  void rawS3PutObjectCrc32cChecksum() {
    String key = String.format("%s/raw-crc32c/%s", runId, UUID.randomUUID());
    byte[] data = "cubed my pineapple".getBytes(StandardCharsets.UTF_8);

    PureJavaCrc32C chk = new PureJavaCrc32C();
    chk.update(data, 0, data.length);
    String chkStr = Base64.getEncoder().encodeToString(Ints.toByteArray((int) chk.getValue()));

    PutObjectRequest req =
        PutObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(key)
            .checksumCRC32C(chkStr)
            .contentLength((long) data.length)
            .build();
    RequestBody body = RequestBody.fromInputStream(new ByteArrayInputStream(data), data.length);
    s3.putObject(req, body);
  }
}
