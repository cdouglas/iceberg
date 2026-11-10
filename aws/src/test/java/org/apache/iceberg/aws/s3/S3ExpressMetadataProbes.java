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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Probes, on a real S3 Express directory bucket, of what an object's incarnation kept in its
 * metadata would rest on (fileio-catalog docs/TAIL_READS.md, "Alternatives considered" and step
 * 0b): a replace sets it with its bytes, an append keeps it, and a GET or a HEAD reports it for the
 * version that served it, while a GET pinned to a version a write superseded serves nothing. Set
 * AWS credentials (and {@code S3_EXPRESS_TEST_BUCKET}); skipped otherwise. What the alternative
 * does not need, but a design would want to know, is printed as {@code PROBE} lines.
 */
public class S3ExpressMetadataProbes {
  private static final String BUCKET =
      System.getenv().getOrDefault("S3_EXPRESS_TEST_BUCKET", "lst-pbafvfgrapl--usw2-az3--x-s3");
  private static final String INCARNATION = "incarnation";

  private static S3Client s3;
  private static String runId;
  private static final List<String> KEYS = Lists.newArrayList();

  @BeforeAll
  static void initStorage() {
    if (System.getenv("AWS_ACCESS_KEY_ID") != null
        && System.getenv("AWS_SECRET_ACCESS_KEY") != null) {
      runId = UUID.randomUUID().toString();
      s3 = AwsClientFactories.defaultFactory().s3();
    }
  }

  @AfterAll
  static void cleanUp() {
    if (s3 != null) {
      for (String key : KEYS) {
        try {
          s3.deleteObject(DeleteObjectRequest.builder().bucket(BUCKET).key(key).build());
        } catch (RuntimeException e) {
          // best effort
        }
      }
      s3.close();
    }
  }

  @BeforeEach
  void requireCredentials() {
    Assumptions.assumeTrue(s3 != null, "AWS credentials not available");
  }

  private static String key(String slug) {
    String key = "metadata-probes/" + runId + "/" + slug;
    KEYS.add(key);
    return key;
  }

  private static byte[] bytes(String content) {
    return content.getBytes(StandardCharsets.UTF_8);
  }

  /** A replace: the whole object, with {@code incarnation}, if the object is {@code ifMatch}. */
  private static String replace(String key, byte[] content, String incarnation, String ifMatch) {
    PutObjectRequest.Builder put =
        PutObjectRequest.builder()
            .bucket(BUCKET)
            .key(key)
            .metadata(Map.of(INCARNATION, incarnation));
    if (ifMatch == null) {
      put.ifNoneMatch("*");
    } else {
      put.ifMatch(ifMatch);
    }
    return s3.putObject(put.build(), RequestBody.fromBytes(content)).eTag();
  }

  /** An append at {@code offset} to the version {@code ifMatch}, as the fork's APPEND sends it. */
  private static String append(String key, byte[] content, long offset, String ifMatch) {
    return s3.putObject(
            PutObjectRequest.builder()
                .bucket(BUCKET)
                .key(key)
                .ifMatch(ifMatch)
                .writeOffsetBytes(offset)
                .build(),
            RequestBody.fromBytes(content))
        .eTag();
  }

  private static HeadObjectResponse head(String key) {
    return s3.headObject(HeadObjectRequest.builder().bucket(BUCKET).key(key).build());
  }

  private static ResponseBytes<GetObjectResponse> get(String key, long from, String ifMatch) {
    GetObjectRequest.Builder get =
        GetObjectRequest.builder().bucket(BUCKET).key(key).range("bytes=" + from + "-");
    if (ifMatch != null) {
      get.ifMatch(ifMatch);
    }
    return s3.getObjectAsBytes(get.build());
  }

  private static int status(Runnable request) {
    try {
      request.run();
      return 200;
    } catch (S3Exception e) {
      return e.statusCode();
    }
  }

  @Test
  void aReplaceSetsTheIncarnationWithItsBytes() {
    String key = key("replace");
    String first = replace(key, bytes("first"), "a", null);
    assertThat(head(key).metadata()).containsEntry(INCARNATION, "a");

    String second = replace(key, bytes("second"), "b", first);
    HeadObjectResponse head = head(key);
    assertThat(head.eTag()).isEqualTo(second);
    assertThat(head.metadata()).containsEntry(INCARNATION, "b");
    ResponseBytes<GetObjectResponse> read = get(key, 0, null);
    assertThat(read.asUtf8String()).isEqualTo("second");
    assertThat(read.response().metadata()).containsEntry(INCARNATION, "b");
  }

  @Test
  void anAppendKeepsTheIncarnation() {
    String key = key("append");
    String created = replace(key, bytes("head"), "a", null);
    String appended = append(key, bytes("-tail"), 4, created);

    HeadObjectResponse head = head(key);
    assertThat(head.eTag()).isEqualTo(appended).isNotEqualTo(created);
    assertThat(head.contentLength()).isEqualTo(9L);
    assertThat(head.metadata()).as("the metadata after an append").containsEntry(INCARNATION, "a");
  }

  /**
   * A ranged GET reports the incarnation, the version and the object's length of the version that
   * served it, in its response: a tail read from N would be one request. A GET pinned to a version
   * an append or a replace superseded serves nothing (step 0's question: whether directory buckets
   * honour If-Match on a GET).
   */
  @Test
  void aRangedGetReportsTheIncarnationOfTheVersionThatServedIt() {
    String key = key("ranged");
    String created = replace(key, bytes("0123456789"), "a", null);
    String appended = append(key, bytes("abcdef"), 10, created);

    ResponseBytes<GetObjectResponse> tail = get(key, 6, null);
    assertThat(tail.asUtf8String()).isEqualTo("6789abcdef");
    assertThat(tail.response().eTag()).isEqualTo(appended);
    assertThat(tail.response().contentRange()).isEqualTo("bytes 6-15/16");
    assertThat(tail.response().metadata()).containsEntry(INCARNATION, "a");
    assertThat(status(() -> get(key, 6, created)))
        .as("a GET pinned to the version before an append")
        .isEqualTo(412);

    String replaced = replace(key, bytes("0123456789abcdefXYZ"), "b", appended);
    assertThat(status(() -> get(key, 6, appended)))
        .as("a GET pinned to the version before a replace")
        .isEqualTo(412);
    ResponseBytes<GetObjectResponse> after = get(key, 6, replaced);
    assertThat(after.asUtf8String()).isEqualTo("6789abcdefXYZ");
    assertThat(after.response().metadata()).containsEntry(INCARNATION, "b");
  }

  /**
   * Whether an append can carry metadata of its own, and what it does to the object's: the
   * alternative needs only that the fork's appends send none, but a store that let an append change
   * it would make every writer's discipline part of the check.
   */
  @Test
  void anAppendThatNamesMetadata() {
    String key = key("append-metadata");
    String created = replace(key, bytes("head"), "a", null);
    String outcome;
    try {
      s3.putObject(
          PutObjectRequest.builder()
              .bucket(BUCKET)
              .key(key)
              .ifMatch(created)
              .writeOffsetBytes(4L)
              .metadata(Map.of(INCARNATION, "z"))
              .build(),
          RequestBody.fromBytes(bytes("-tail")));
      outcome = "accepted";
    } catch (S3Exception e) {
      outcome =
          "rejected "
              + e.statusCode()
              + " "
              + e.awsErrorDetails().errorCode()
              + ": "
              + e.awsErrorDetails().errorMessage();
    }
    HeadObjectResponse head = head(key);
    System.out.println(
        "PROBE s3express append-with-metadata: "
            + outcome
            + "; afterwards length "
            + head.contentLength()
            + ", metadata "
            + head.metadata());
  }
}
