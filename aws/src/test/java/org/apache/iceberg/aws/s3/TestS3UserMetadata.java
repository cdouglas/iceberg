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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/**
 * Smoke tests that verify S3 round-trips user-defined object metadata (HTTP {@code x-amz-meta-*}
 * headers) on both bucket flavors:
 *
 * <ul>
 *   <li>standard bucket — plain PutObject;
 *   <li>S3 Express One Zone (directory) bucket — PutObject with If-Match CAS precondition;
 *   <li>S3 Express One Zone — PutObject with {@code writeOffsetBytes} append, verifying the
 *       metadata set on the initial write survives the append.
 * </ul>
 *
 * <p>Run with AWS credentials available; otherwise tests skip. Buckets default to the same fixtures
 * used by {@link S3StandardFileIOAtomicTest} / {@link S3ExpressFileIOAtomicTest}.
 */
public class TestS3UserMetadata {

  private static final String STD_BUCKET =
      System.getenv().getOrDefault("S3_TEST_BUCKET", "lst-pbafvfgrapl");
  private static final String EXPR_BUCKET =
      System.getenv().getOrDefault("S3_EXPRESS_TEST_BUCKET", "lst-pbafvfgrapl--usw2-az3--x-s3");

  private static final String META_KEY = "user-defined-yaks";
  private static final String META_VALUE = "dingos";
  private static final Map<String, String> USER_META = ImmutableMap.of(META_KEY, META_VALUE);

  private static String runId;
  private static S3Client s3;

  @BeforeAll
  static void initStorage() {
    // Probe the default AWS credentials provider chain (env vars, profile, instance role, …).
    // Skip if no credentials are resolvable; do not depend on AWS_ACCESS_KEY_ID being exported.
    try {
      DefaultCredentialsProvider.create().resolveCredentials();
      runId = UUID.randomUUID().toString();
      s3 = AwsClientFactories.defaultFactory().s3();
    } catch (SdkException e) {
      // credentials unavailable; tests will skip in @BeforeEach.
    }
  }

  @BeforeEach
  void requireCredentials() {
    Assumptions.assumeTrue(s3 != null, "AWS credentials not available");
  }

  /** Baseline: standard bucket round-trips {@code x-amz-meta-user-defined-yaks=dingos}. */
  @Test
  void standardBucketRoundTripsUserMetadata() {
    String key = String.format("%s/user-meta-std/%s", runId, UUID.randomUUID());
    byte[] data = "yaks have been here".getBytes(StandardCharsets.UTF_8);

    PutObjectRequest put =
        PutObjectRequest.builder()
            .bucket(STD_BUCKET)
            .key(key)
            .metadata(USER_META)
            .contentLength((long) data.length)
            .build();
    s3.putObject(put, RequestBody.fromBytes(data));

    HeadObjectResponse head =
        s3.headObject(HeadObjectRequest.builder().bucket(STD_BUCKET).key(key).build());
    // SDK lowercases keys and strips the x-amz-meta- prefix.
    assertThat(head.metadata()).containsEntry(META_KEY, META_VALUE);
  }

  /**
   * S3 Express One Zone: PutObject with an If-Match CAS precondition preserves user-defined
   * metadata across the replace. First write asserts absent (ifNoneMatch=*); second pins the live
   * etag and overwrites with a fresh metadata value.
   */
  @Test
  void expressBucketCasPreservesUserMetadata() {
    String key = String.format("%s/user-meta-cas/%s", runId, UUID.randomUUID());
    byte[] v1 = "first dingo".getBytes(StandardCharsets.UTF_8);

    PutObjectRequest create =
        PutObjectRequest.builder()
            .bucket(EXPR_BUCKET)
            .key(key)
            .ifNoneMatch("*")
            .metadata(USER_META)
            .contentLength((long) v1.length)
            .build();
    PutObjectResponse createResp = s3.putObject(create, RequestBody.fromBytes(v1));

    HeadObjectResponse head1 =
        s3.headObject(HeadObjectRequest.builder().bucket(EXPR_BUCKET).key(key).build());
    assertThat(head1.metadata()).containsEntry(META_KEY, META_VALUE);

    // CAS replace with a different metadata value, pinning the current etag.
    byte[] v2 = "second dingo".getBytes(StandardCharsets.UTF_8);
    Map<String, String> updated = ImmutableMap.of(META_KEY, "more-" + META_VALUE);
    PutObjectRequest cas =
        PutObjectRequest.builder()
            .bucket(EXPR_BUCKET)
            .key(key)
            .ifMatch(createResp.eTag())
            .metadata(updated)
            .contentLength((long) v2.length)
            .build();
    s3.putObject(cas, RequestBody.fromBytes(v2));

    HeadObjectResponse head2 =
        s3.headObject(HeadObjectRequest.builder().bucket(EXPR_BUCKET).key(key).build());
    assertThat(head2.metadata()).containsEntry(META_KEY, "more-" + META_VALUE);
  }

  /**
   * S3 Express One Zone: PutObject with {@code writeOffsetBytes} appends to an existing object;
   * verifies the user-defined metadata set on the initial CAS write is preserved across the append
   * (the append request does not re-send metadata).
   */
  @Test
  void expressBucketAppendPreservesUserMetadata() {
    String key = String.format("%s/user-meta-append/%s", runId, UUID.randomUUID());
    byte[] head = "head-".getBytes(StandardCharsets.UTF_8);
    byte[] tail = "tail".getBytes(StandardCharsets.UTF_8);

    // Initial write carries the user-defined metadata.
    PutObjectRequest create =
        PutObjectRequest.builder()
            .bucket(EXPR_BUCKET)
            .key(key)
            .ifNoneMatch("*")
            .metadata(USER_META)
            .contentLength((long) head.length)
            .build();
    PutObjectResponse createResp = s3.putObject(create, RequestBody.fromBytes(head));

    // Append. No metadata on the append PUT itself.
    PutObjectRequest append =
        PutObjectRequest.builder()
            .bucket(EXPR_BUCKET)
            .key(key)
            .ifMatch(createResp.eTag())
            .writeOffsetBytes((long) head.length)
            .contentLength((long) tail.length)
            .build();
    s3.putObject(append, RequestBody.fromBytes(tail));

    HeadObjectResponse headResp =
        s3.headObject(HeadObjectRequest.builder().bucket(EXPR_BUCKET).key(key).build());
    assertThat(headResp.contentLength()).isEqualTo((long) (head.length + tail.length));
    assertThat(headResp.metadata()).containsEntry(META_KEY, META_VALUE);
  }
}
