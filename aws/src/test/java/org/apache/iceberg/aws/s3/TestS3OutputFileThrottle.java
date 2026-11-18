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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * A conditional write that S3 throttles, or fails with an error it asks callers to retry, fails as
 * {@link SupportsAtomicOperations.StorageThrottleException}, for a CAS and an append alike, so a
 * caller backs off before it sends the write again. Other failures keep their mapping.
 */
class TestS3OutputFileThrottle {

  private static final String LOCATION = "s3://bucket--use1-az4--x-s3/key";
  private static final byte[] BYTES = "written".getBytes(StandardCharsets.UTF_8);

  private S3Client s3;
  private S3FileIO io;

  @BeforeEach
  void before() {
    s3 = mock(S3Client.class);
    io = new S3FileIO(() -> s3);
    io.initialize(Map.of());
  }

  @ParameterizedTest
  @CsvSource({
    "CAS, 503, SlowDown",
    "APPEND, 503, SlowDown",
    "CAS, 503, ServiceUnavailable",
    "APPEND, 503, ServiceUnavailable",
    "CAS, 500, InternalError",
    "APPEND, 500, InternalError",
    "CAS, 504, GatewayTimeout",
    "CAS, 429, TooManyRequests",
    "APPEND, 400, Throttling"
  })
  void aThrottledWriteFailsAsAThrottle(AtomicOutputFile.Strategy strategy, int status, String code)
      throws IOException {
    S3Exception refused = s3Exception(status, code);
    when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(refused);

    assertThatThrownBy(() -> write(pinned(), strategy))
        .isInstanceOf(SupportsAtomicOperations.StorageThrottleException.class)
        .hasMessageContaining(String.valueOf(status))
        .hasMessageContaining(code)
        .cause()
        .isSameAs(refused);
  }

  @ParameterizedTest
  @CsvSource({
    "CAS, 400, InvalidRequest",
    "APPEND, 400, InvalidRequest",
    "APPEND, 400, InvalidArgument",
    "CAS, 403, AccessDenied",
    "APPEND, 501, NotImplemented"
  })
  void aRefusedWriteIsNoThrottle(AtomicOutputFile.Strategy strategy, int status, String code)
      throws IOException {
    S3Exception refused = s3Exception(status, code);
    when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(refused);

    assertThatThrownBy(() -> write(pinned(), strategy)).isSameAs(refused);
  }

  /** A failed condition is a conflict, as it was: neither a throttle nor the SDK's exception. */
  @ParameterizedTest
  @CsvSource({
    "CAS, 412, PreconditionFailed, org.apache.iceberg.io.SupportsAtomicOperations$CASException",
    "CAS, 409, ConditionalRequestConflict,"
        + " org.apache.iceberg.io.SupportsAtomicOperations$CASException",
    "APPEND, 412, PreconditionFailed,"
        + " org.apache.iceberg.io.SupportsAtomicOperations$AppendException",
    "APPEND, 409, ConditionalRequestConflict,"
        + " org.apache.iceberg.io.SupportsAtomicOperations$AppendException"
  })
  void aFailedConditionIsAConflict(
      AtomicOutputFile.Strategy strategy, int status, String code, Class<?> expected)
      throws IOException {
    when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenThrow(s3Exception(status, code));

    assertThatThrownBy(() -> write(pinned(), strategy))
        .isExactlyInstanceOf(expected)
        .isNotInstanceOf(SupportsAtomicOperations.StorageThrottleException.class);
  }

  /** An append from a snapshot that has seen no version looks the object up: a throttle too. */
  @Test
  void aThrottledLookupBeforeAnAppendFailsAsAThrottle() {
    // a HEAD's error has no body: the status alone
    S3Exception refused = s3Exception(503, null);
    when(s3.headObject(any(HeadObjectRequest.class))).thenThrow(refused);

    assertThatThrownBy(() -> write(io.newInputFile(LOCATION), AtomicOutputFile.Strategy.APPEND))
        .isInstanceOf(SupportsAtomicOperations.StorageThrottleException.class)
        .cause()
        .isSameAs(refused);
  }

  private static S3Exception s3Exception(int status, String code) {
    return (S3Exception)
        S3Exception.builder()
            .statusCode(status)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode(code).build())
            .message("refused")
            .build();
  }

  private InputFile pinned() {
    return new S3InputFile(
        s3,
        null,
        new S3URI(LOCATION),
        10L,
        new S3FileIOProperties(),
        MetricsContext.nullMetrics(),
        "\"first\"");
  }

  private InputFile write(InputFile snapshot, AtomicOutputFile.Strategy strategy)
      throws IOException {
    AtomicOutputFile out = io.newOutputFile(snapshot);
    CAS token = out.prepare(() -> new ByteArrayInputStream(BYTES), strategy);
    return out.writeAtomic(token, () -> new ByteArrayInputStream(BYTES));
  }
}
