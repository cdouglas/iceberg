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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Locale;
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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * A conditional write that S3 refuses with a client error the same request gets again fails as
 * {@link SupportsAtomicOperations.StorageRefusedException}: nothing was written. An error that does
 * not prove that (a conflict, a 404, a timeout, a clock-skew or transient error, or any error that
 * ends the SDK's retries of the write, which an earlier attempt may have landed before) keeps its
 * mapping, or propagates as it is.
 */
class TestS3OutputFileRefused {

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
    "CAS, 400, InvalidRequest",
    "APPEND, 400, InvalidArgument",
    "CAS, 400, BadDigest",
    "CAS, 400, ExpiredToken",
    "CAS, 401, Unauthorized",
    "CAS, 403, AccessDenied",
    "APPEND, 403, AccessDenied",
    "CAS, 403, SignatureDoesNotMatch",
    "CAS, 405, MethodNotAllowed",
    "CAS, 411, MissingContentLength",
    "APPEND, 413, EntityTooLarge",
    "APPEND, 501, NotImplemented",
    "CAS, 501, NotImplemented"
  })
  void aWriteRefusedOnItsOnlyAttemptIsRefused(
      AtomicOutputFile.Strategy strategy, int status, String code) throws IOException {
    S3Exception refused = s3Exception(status, code, 1);
    when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(refused);

    assertThatThrownBy(() -> write(pinned(), strategy))
        .isExactlyInstanceOf(SupportsAtomicOperations.StorageRefusedException.class)
        .hasMessageContaining(status + " " + code)
        .cause()
        .isSameAs(refused);
  }

  /**
   * A 404 says nothing of the write here; 408 and 499 that the request may not have arrived whole;
   * an unmapped 409 is a conflict; S3's RequestTimeout and PriorRequestNotComplete, and clock skew,
   * are transient errors the SDK retries. None is a refusal: each propagates as it is.
   */
  @ParameterizedTest
  @CsvSource({
    "CAS, 404, NoSuchBucket",
    "CAS, 408, RequestTimeout",
    "APPEND, 400, RequestTimeout",
    "CAS, 400, RequestTimeoutException",
    "CAS, 400, PriorRequestNotComplete",
    "CAS, 403, RequestTimeTooSkewed",
    "CAS, 403, RequestExpired",
    "CAS, 409, OperationAborted",
    "CAS, 499, ClientClosedRequest"
  })
  void anErrorThatProvesNothingIsNoRefusal(
      AtomicOutputFile.Strategy strategy, int status, String code) throws IOException {
    S3Exception error = s3Exception(status, code, 1);
    when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(error);

    assertThatThrownBy(() -> write(pinned(), strategy)).isSameAs(error);
  }

  /**
   * A client error that ends the SDK's retries of a write is no refusal: an earlier attempt may
   * have landed (its reply lost, or a 5xx after S3 applied it), and the last attempt's error (here
   * credentials that went bad between attempts) says nothing of it.
   */
  @ParameterizedTest
  @CsvSource({"CAS, 403, AccessDenied", "APPEND, 400, ExpiredToken", "CAS, 501, NotImplemented"})
  void aClientErrorAfterTheSdksRetriesIsNoRefusal(
      AtomicOutputFile.Strategy strategy, int status, String code) throws IOException {
    S3Exception error = s3Exception(status, code, 2);
    when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(error);

    assertThatThrownBy(() -> write(pinned(), strategy)).isSameAs(error);
  }

  /** An error with no attempt count did not come through the SDK's retries: it proves nothing. */
  @Test
  void aClientErrorWithNoAttemptCountIsNoRefusal() throws IOException {
    S3Exception error = s3Exception(403, "AccessDenied", null);
    when(s3.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenThrow(error);

    assertThatThrownBy(() -> write(pinned(), AtomicOutputFile.Strategy.CAS)).isSameAs(error);
  }

  /** An append from a snapshot that has seen no version looks the object up first. */
  @Test
  void aRefusedLookupBeforeAnAppendIsRefused() {
    // a HEAD's error has no body: the status alone
    S3Exception refused = s3Exception(403, null, 1);
    when(s3.headObject(any(HeadObjectRequest.class))).thenThrow(refused);

    assertThatThrownBy(() -> write(io.newInputFile(LOCATION), AtomicOutputFile.Strategy.APPEND))
        .isExactlyInstanceOf(SupportsAtomicOperations.StorageRefusedException.class)
        .cause()
        .isSameAs(refused);
  }

  /**
   * The lookup is a read: whatever attempt refused it, the write was not sent. Its 404 keeps its
   * meaning: there is no object to append to.
   */
  @Test
  void aLookupRefusedAfterRetriesIsRefusedAndItsNotFoundIsAnAppendFailure() {
    S3Exception refused = s3Exception(403, null, 3);
    when(s3.headObject(any(HeadObjectRequest.class))).thenThrow(refused);
    assertThatThrownBy(() -> write(io.newInputFile(LOCATION), AtomicOutputFile.Strategy.APPEND))
        .isExactlyInstanceOf(SupportsAtomicOperations.StorageRefusedException.class);

    S3Exception missing = s3Exception(404, null, 1);
    when(s3.headObject(any(HeadObjectRequest.class))).thenThrow(missing);
    assertThatThrownBy(() -> write(io.newInputFile(LOCATION), AtomicOutputFile.Strategy.APPEND))
        .isExactlyInstanceOf(SupportsAtomicOperations.AppendException.class);
  }

  /**
   * Through the SDK: a 403 on the write's only attempt is a refusal, and the SDK counts the attempt
   * on the exception it throws.
   */
  @Test
  void theSdkCountsAWriteRefusedOnItsOnlyAttempt() {
    StubHttp http = new StubHttp().answer(403, "AccessDenied");
    try (S3Client client = client(http)) {
      assertThatThrownBy(() -> write(client, AtomicOutputFile.Strategy.CAS))
          .isExactlyInstanceOf(SupportsAtomicOperations.StorageRefusedException.class)
          .cause()
          .isInstanceOfSatisfying(S3Exception.class, e -> assertThat(e.numAttempts()).isEqualTo(1));
    }
    assertThat(http.requests).isEqualTo(1);
  }

  /**
   * Through the SDK: a 503 the SDK retries, then a 403. The first attempt may have landed (a 5xx
   * can follow a write S3 applied), so the 403 that ends the retries is no refusal.
   */
  @Test
  void theSdkCountsTheAttemptsOfAWriteItRetried() {
    StubHttp http = new StubHttp().answer(503, "ServiceUnavailable").answer(403, "AccessDenied");
    try (S3Client client = client(http)) {
      // the SDK models S3's AccessDenied as a subclass of S3Exception
      assertThatThrownBy(() -> write(client, AtomicOutputFile.Strategy.CAS))
          .isInstanceOf(S3Exception.class)
          .satisfies(e -> assertThat(((S3Exception) e).numAttempts()).isEqualTo(2))
          .satisfies(e -> assertThat(((S3Exception) e).statusCode()).isEqualTo(403));
    }
    assertThat(http.requests).isEqualTo(2);
  }

  private static S3Exception s3Exception(int status, String code, Integer attempts) {
    return (S3Exception)
        S3Exception.builder()
            .statusCode(status)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode(code).build())
            .message("refused")
            .numAttempts(attempts)
            .build();
  }

  private InputFile pinned() {
    return pinned(s3, LOCATION);
  }

  private static InputFile pinned(S3Client client, String location) {
    return new S3InputFile(
        client,
        null,
        new S3URI(location),
        10L,
        new S3FileIOProperties(),
        MetricsContext.nullMetrics(),
        "\"first\"");
  }

  private InputFile write(InputFile snapshot, AtomicOutputFile.Strategy strategy)
      throws IOException {
    return write(io, snapshot, strategy);
  }

  private static InputFile write(
      S3FileIO fileIO, InputFile snapshot, AtomicOutputFile.Strategy strategy) throws IOException {
    AtomicOutputFile out = fileIO.newOutputFile(snapshot);
    CAS token = out.prepare(() -> new ByteArrayInputStream(BYTES), strategy);
    return out.writeAtomic(token, () -> new ByteArrayInputStream(BYTES));
  }

  private static InputFile write(S3Client client, AtomicOutputFile.Strategy strategy)
      throws IOException {
    S3FileIO fileIO = new S3FileIO(() -> client);
    fileIO.initialize(Map.of());
    return write(fileIO, pinned(client, "s3://bucket/key"), strategy);
  }

  /** An S3 client whose requests reach {@code http}, retrying without a wait. */
  private static S3Client client(StubHttp http) {
    return S3Client.builder()
        .httpClient(http)
        .region(Region.US_EAST_1)
        .endpointOverride(URI.create("http://localhost:9"))
        .forcePathStyle(true)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("key", "secret")))
        .overrideConfiguration(
            c ->
                c.retryStrategy(
                    b ->
                        b.maxAttempts(3)
                            .backoffStrategy(BackoffStrategy.retryImmediately())
                            .throttlingBackoffStrategy(BackoffStrategy.retryImmediately())))
        .build();
  }

  /** An HTTP client that answers each request with the next error queued, as S3 would. */
  private static final class StubHttp implements SdkHttpClient {
    private final Deque<Object[]> answers = new ArrayDeque<>();
    private int requests = 0;

    StubHttp answer(int status, String code) {
      answers.add(new Object[] {status, code});
      return this;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return new ExecutableHttpRequest() {
        @Override
        public HttpExecuteResponse call() {
          requests += 1;
          request
              .contentStreamProvider()
              .ifPresent(
                  body -> {
                    try (InputStream in = body.newStream()) {
                      in.transferTo(OutputStream.nullOutputStream());
                    } catch (IOException e) {
                      throw new UncheckedIOException(e);
                    }
                  });
          Object[] answer = answers.remove();
          byte[] body =
              String.format(
                      Locale.ROOT,
                      "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>%s</Code>"
                          + "<Message>refused</Message><RequestId>r</RequestId></Error>",
                      answer[1])
                  .getBytes(StandardCharsets.UTF_8);
          return HttpExecuteResponse.builder()
              .response(
                  SdkHttpResponse.builder()
                      .statusCode((Integer) answer[0])
                      .putHeader("Content-Type", "application/xml")
                      .putHeader("Content-Length", String.valueOf(body.length))
                      .build())
              .responseBody(AbortableInputStream.create(new ByteArrayInputStream(body)))
              .build();
        }

        @Override
        public void abort() {}
      };
    }

    @Override
    public void close() {}
  }
}
