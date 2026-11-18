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
package org.apache.iceberg.azure.adlsv2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.Context;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RequestRetryPolicy;
import com.azure.storage.common.policy.RetryPolicyType;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.stubbing.Answer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A conditional write that Azure refuses with a client error the same request gets again fails as
 * {@link SupportsAtomicOperations.StorageRefusedException}: nothing was written. An error that does
 * not prove that (a conflict, a 404, a timeout, or a client error that ends the SDK's retries of
 * the write, which an earlier attempt may have landed before) keeps its mapping, or propagates as
 * it is. The Blob API's requests go through the SDK to a stub of the service, which answers each as
 * a test says; the pipeline counts their attempts as ADLSFileIO's clients do ({@link
 * ADLSAttempts}).
 */
class TestADLSOutputFileRefused {

  private static final String LOCATION = "abfs://container@account.dfs.core.windows.net/dir/file";
  private static final String URL = "https://account.dfs.core.windows.net/container/dir/file";
  private static final String TEMP_URL =
      "https://account.dfs.core.windows.net/container/dir/.file.cas-0.tmp";
  private static final byte[] BYTES = "written".getBytes(StandardCharsets.UTF_8);

  @ParameterizedTest
  @CsvSource({
    "400, Md5Mismatch",
    "400, InvalidHeaderValue",
    "401, NoAuthenticationInformation",
    "403, AuthorizationFailure",
    "403, AuthenticationFailed",
    "405, UnsupportedHttpVerb",
    "411, MissingContentLengthHeader",
    "413, RequestBodyTooLarge",
    "501, NotImplemented"
  })
  void anAppendRefusedOnItsOnlyAttemptIsRefused(int status, String code) {
    DataLakeFileClient file = file(URL, counted(), request -> error(request, status, code));

    assertThatThrownBy(() -> write(pinned(file, null), AtomicOutputFile.Strategy.APPEND))
        .isExactlyInstanceOf(SupportsAtomicOperations.StorageRefusedException.class)
        .hasMessageContaining(status + " " + code)
        .cause()
        .isInstanceOf(BlobStorageException.class)
        .extracting(e -> ((BlobStorageException) e).getStatusCode())
        .isEqualTo(status);
  }

  /**
   * 408 and 499 say the request may not have arrived whole; an unmapped 409 (a blob written through
   * the Data Lake API) is a conflict. None is a refusal.
   */
  @ParameterizedTest
  @CsvSource({"408, RequestTimeout", "409, InvalidBlobType", "499, ClientClosedRequest"})
  void anAppendErrorThatProvesNothingIsNoRefusal(int status, String code) {
    DataLakeFileClient file = file(URL, counted(), request -> error(request, status, code));

    assertThatThrownBy(() -> write(pinned(file, null), AtomicOutputFile.Strategy.APPEND))
        .isExactlyInstanceOf(BlobStorageException.class)
        .extracting(e -> ((BlobStorageException) e).getStatusCode())
        .isEqualTo(status);
  }

  /** An append to a file that is gone is a failed append, as it was. */
  @Test
  void anAppendToAFileThatIsGoneIsAFailedAppend() {
    DataLakeFileClient file = file(URL, counted(), request -> error(request, 404, "BlobNotFound"));

    assertThatThrownBy(() -> write(pinned(file, null), AtomicOutputFile.Strategy.APPEND))
        .isExactlyInstanceOf(SupportsAtomicOperations.AppendException.class);
  }

  /**
   * Through the SDK's retry policy: a 503, which the SDK retries, then a 403. The first attempt may
   * have landed (a 5xx can follow an append Azure applied), so the 403 that ends the retries is no
   * refusal. On its own, through the same pipeline, the 403 is one.
   */
  @Test
  void aClientErrorAfterTheSdksRetriesIsNoRefusal() {
    Deque<Integer> statuses = new ArrayDeque<>(List.of(503, 403));
    AtomicInteger sent = new AtomicInteger();
    DataLakeFileClient retried =
        file(
            URL,
            List.of(retries(), new ADLSAttempts()),
            request -> {
              sent.incrementAndGet();
              int status = statuses.remove();
              return error(request, status, status == 503 ? "ServerBusy" : "AuthorizationFailure");
            });

    assertThatThrownBy(() -> write(pinned(retried, null), AtomicOutputFile.Strategy.APPEND))
        .isExactlyInstanceOf(BlobStorageException.class)
        .extracting(e -> ((BlobStorageException) e).getStatusCode())
        .isEqualTo(403);
    assertThat(sent).hasValue(2);

    DataLakeFileClient once =
        file(
            URL,
            List.of(retries(), new ADLSAttempts()),
            request -> error(request, 403, "AuthorizationFailure"));
    assertThatThrownBy(() -> write(pinned(once, null), AtomicOutputFile.Strategy.APPEND))
        .isExactlyInstanceOf(SupportsAtomicOperations.StorageRefusedException.class);
  }

  /** A client that counts no attempts (not built by ADLSFileIO) cannot say: no refusal. */
  @Test
  void aClientErrorWithNoAttemptCountIsNoRefusal() {
    DataLakeFileClient file =
        file(URL, List.of(), request -> error(request, 403, "AuthorizationFailure"));

    assertThatThrownBy(() -> write(pinned(file, null), AtomicOutputFile.Strategy.APPEND))
        .isExactlyInstanceOf(BlobStorageException.class);
  }

  /** An append from a snapshot that has seen no version looks the file up: a refusal too. */
  @Test
  void aRefusedLookupBeforeAnAppendIsRefused() {
    DataLakeFileClient file = file(URL, counted(), request -> error(request, 500, "InternalError"));
    DataLakeStorageException refused = dataLakeError(403, "AuthorizationFailure");
    when(file.getProperties()).thenThrow(refused);

    assertThatThrownBy(() -> write(unseen(file), AtomicOutputFile.Strategy.APPEND))
        .isExactlyInstanceOf(SupportsAtomicOperations.StorageRefusedException.class)
        .cause()
        .isSameAs(refused);
  }

  /**
   * A CAS stages its content in a temp blob first: a refused staging leaves the target untouched,
   * whatever attempt it ended on.
   */
  @Test
  void aRefusedStagingIsRefused() {
    DataLakeFileClient file = file(URL, counted(), request -> error(request, 500, "InternalError"));
    DataLakeFileClient temp =
        file(TEMP_URL, List.of(), request -> error(request, 403, "AuthorizationFailure"));

    assertThatThrownBy(() -> write(pinned(file, temp), AtomicOutputFile.Strategy.CAS))
        .isExactlyInstanceOf(SupportsAtomicOperations.StorageRefusedException.class)
        .hasMessageContaining("403 AuthorizationFailure")
        .cause()
        .isInstanceOf(BlobStorageException.class);
  }

  /** The rename onto the target ends a CAS: refused on its only attempt, nothing landed. */
  @ParameterizedTest
  @CsvSource({
    "400, InvalidRenameSourcePath",
    "403, AuthorizationPermissionMismatch",
    "501, NotImplemented"
  })
  void aRenameRefusedOnItsOnlyAttemptIsRefused(int status, String code) {
    DataLakeFileClient file = file(URL, counted(), request -> error(request, 500, "InternalError"));
    DataLakeFileClient temp = file(TEMP_URL, counted(), TestADLSOutputFileRefused::staged);
    DataLakeStorageException refused = dataLakeError(status, code);
    doAnswer(attempts(1, refused))
        .when(temp)
        .renameWithResponse(any(), any(), any(), any(), any(), any());

    assertThatThrownBy(() -> write(pinned(file, temp), AtomicOutputFile.Strategy.CAS))
        .isExactlyInstanceOf(SupportsAtomicOperations.StorageRefusedException.class)
        .hasMessageContaining(status + " " + code)
        .cause()
        .isSameAs(refused);
  }

  /**
   * A rename the SDK sent again may have landed on an earlier attempt: its 403 is no refusal. Nor
   * is a 404, which is how a rename's later attempt finds the source an earlier one moved, nor a
   * rename that counted no attempts.
   */
  @Test
  void aRenameErrorThatProvesNothingIsNoRefusal() {
    DataLakeFileClient file = file(URL, counted(), request -> error(request, 500, "InternalError"));
    DataLakeFileClient temp = file(TEMP_URL, counted(), TestADLSOutputFileRefused::staged);

    DataLakeStorageException denied = dataLakeError(403, "AuthorizationPermissionMismatch");
    doAnswer(attempts(2, denied))
        .when(temp)
        .renameWithResponse(any(), any(), any(), any(), any(), any());
    assertThatThrownBy(() -> write(pinned(file, temp), AtomicOutputFile.Strategy.CAS))
        .isSameAs(denied);

    DataLakeStorageException gone = dataLakeError(404, "SourcePathNotFound");
    doAnswer(attempts(1, gone))
        .when(temp)
        .renameWithResponse(any(), any(), any(), any(), any(), any());
    assertThatThrownBy(() -> write(pinned(file, temp), AtomicOutputFile.Strategy.CAS))
        .isSameAs(gone);

    doAnswer(attempts(0, denied))
        .when(temp)
        .renameWithResponse(any(), any(), any(), any(), any(), any());
    assertThatThrownBy(() -> write(pinned(file, temp), AtomicOutputFile.Strategy.CAS))
        .isSameAs(denied);
  }

  /** ADLSFileIO's clients count each attempt: the counter sits after the SDK's retry policy. */
  @Test
  void theFileIOsClientsCountAttempts() {
    ADLSFileIO io = new ADLSFileIO();
    io.initialize(
        Map.of(AzureProperties.ADLS_SAS_TOKEN_PREFIX + "account.dfs.core.windows.net", "sv=x"));
    HttpPipeline pipeline =
        io.client("abfs://container@account.dfs.core.windows.net/dir/file").getHttpPipeline();
    List<HttpPipelinePolicy> policies = Lists.newArrayList();
    for (int i = 0; i < pipeline.getPolicyCount(); i++) {
      policies.add(pipeline.getPolicy(i));
    }

    int retries = Iterables.indexOf(policies, policy -> policy instanceof RequestRetryPolicy);
    int counter = Iterables.indexOf(policies, policy -> policy instanceof ADLSAttempts);
    assertThat(retries).isNotNegative();
    assertThat(counter).isGreaterThan(retries);
  }

  /** A rename that, as the SDK would, counts {@code n} attempts in its context, then fails. */
  private static Answer<Object> attempts(int n, RuntimeException error) {
    return invocation -> {
      Context context = invocation.getArgument(5);
      context
          .getData(ADLSAttempts.class.getName())
          .ifPresent(attempts -> ((AtomicInteger) attempts).addAndGet(n));
      throw error;
    };
  }

  private static List<HttpPipelinePolicy> counted() {
    return List.of(new ADLSAttempts());
  }

  /** The storage SDK's retry policy, waiting a millisecond between attempts. */
  private static HttpPipelinePolicy retries() {
    return new RequestRetryPolicy(
        new RequestRetryOptions(
            RetryPolicyType.FIXED,
            3,
            (Duration) null,
            Duration.ofMillis(1),
            Duration.ofMillis(1),
            null));
  }

  /** An output file pinned to version {@code e0} of 10 bytes; a CAS stages through {@code temp}. */
  private static ADLSOutputFile pinned(DataLakeFileClient file, DataLakeFileClient temp) {
    return new ADLSOutputFile(
        LOCATION,
        file,
        new AzureProperties(),
        10L,
        new DataLakeRequestConditions().setIfMatch("\"e0\""),
        MetricsContext.nullMetrics(),
        location -> temp);
  }

  /** An output file from a snapshot that has seen no version: an append looks the file up. */
  private static ADLSOutputFile unseen(DataLakeFileClient file) {
    return new ADLSOutputFile(
        LOCATION,
        file,
        new AzureProperties(),
        -1L,
        new DataLakeRequestConditions().setIfNoneMatch("*"),
        MetricsContext.nullMetrics(),
        location -> file,
        true);
  }

  private static void write(AtomicOutputFile out, AtomicOutputFile.Strategy strategy)
      throws IOException {
    CAS token = out.prepare(() -> new ByteArrayInputStream(BYTES), strategy);
    out.writeAtomic(token, () -> new ByteArrayInputStream(BYTES));
  }

  /**
   * A Data Lake client whose Blob API requests go through {@code policies} to the stub, which
   * answers with {@code respond}.
   */
  private static DataLakeFileClient file(
      String url, List<HttpPipelinePolicy> policies, Function<HttpRequest, HttpResponse> respond) {
    HttpPipeline pipeline =
        new HttpPipelineBuilder()
            .policies(policies.toArray(new HttpPipelinePolicy[0]))
            .httpClient(request -> Mono.fromCallable(() -> respond.apply(request)))
            .build();
    DataLakeFileClient file = mock(DataLakeFileClient.class);
    when(file.getHttpPipeline()).thenReturn(pipeline);
    when(file.getFileUrl()).thenReturn(url);
    when(file.getFilePath()).thenReturn(url.substring(url.indexOf("/container/") + 11));
    return file;
  }

  /** The stub staging a temp blob: it creates it, appends to it, and deletes it. */
  private static HttpResponse staged(HttpRequest request) {
    HttpHeaders headers =
        new HttpHeaders()
            .set(HttpHeaderName.ETAG, "\"t1\"")
            .set(HttpHeaderName.LAST_MODIFIED, "Tue, 17 Nov 2026 00:00:00 GMT")
            .set(HttpHeaderName.fromString("x-ms-request-server-encrypted"), "true");
    if (request.getHttpMethod() == HttpMethod.DELETE) {
      return new StubResponse(request, 202, headers, new byte[0]);
    }
    if (request.getUrl().getQuery() != null
        && request.getUrl().getQuery().contains("comp=appendblock")) {
      headers
          .set(HttpHeaderName.fromString("x-ms-blob-append-offset"), "0")
          .set(HttpHeaderName.fromString("x-ms-blob-committed-block-count"), "1");
    }
    return new StubResponse(request, 201, headers, new byte[0]);
  }

  private static HttpResponse error(HttpRequest request, int status, String code) {
    HttpHeaders headers =
        new HttpHeaders()
            .set(HttpHeaderName.fromString("x-ms-error-code"), code)
            .set(HttpHeaderName.CONTENT_TYPE, "application/xml");
    String body =
        String.format(
            Locale.ROOT,
            "<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>%s</Code>"
                + "<Message>refused</Message></Error>",
            code);
    return new StubResponse(request, status, headers, body.getBytes(StandardCharsets.UTF_8));
  }

  private static DataLakeStorageException dataLakeError(int status, String code) {
    HttpRequest request = new HttpRequest(HttpMethod.PUT, URL);
    return new DataLakeStorageException(code, error(request, status, code), null);
  }

  private static final class StubResponse extends HttpResponse {
    private final int status;
    private final HttpHeaders headers;
    private final byte[] body;

    StubResponse(HttpRequest request, int status, HttpHeaders headers, byte[] body) {
      super(request);
      this.status = status;
      this.headers = headers;
      this.body = body;
    }

    @Override
    public int getStatusCode() {
      return status;
    }

    @Override
    public String getHeaderValue(String name) {
      return headers.getValue(HttpHeaderName.fromString(name));
    }

    @Override
    public HttpHeaders getHeaders() {
      return headers;
    }

    @Override
    public Flux<ByteBuffer> getBody() {
      return body.length == 0 ? Flux.empty() : Flux.defer(() -> Flux.just(ByteBuffer.wrap(body)));
    }

    @Override
    public Mono<byte[]> getBodyAsByteArray() {
      return Mono.just(body);
    }

    @Override
    public Mono<String> getBodyAsString() {
      return Mono.just(new String(body, StandardCharsets.UTF_8));
    }

    @Override
    public Mono<String> getBodyAsString(Charset charset) {
      return Mono.just(new String(body, charset));
    }
  }
}
