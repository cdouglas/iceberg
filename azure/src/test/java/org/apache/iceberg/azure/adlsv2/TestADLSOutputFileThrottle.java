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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.function.Function;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A conditional write that Azure throttles, or fails with an error it asks callers to retry, fails
 * as {@link SupportsAtomicOperations.StorageThrottleException}, for a CAS and an append alike, so a
 * caller backs off before it sends the write again. Other failures keep their mapping. The Blob
 * API's requests go through the SDK to a stub of the service, which answers each as a test says.
 */
class TestADLSOutputFileThrottle {

  private static final String LOCATION = "abfs://container@account.dfs.core.windows.net/dir/file";
  private static final String URL = "https://account.dfs.core.windows.net/container/dir/file";
  private static final String TEMP_URL =
      "https://account.dfs.core.windows.net/container/dir/.file.cas-0.tmp";
  private static final byte[] BYTES = "written".getBytes(StandardCharsets.UTF_8);

  @ParameterizedTest
  @CsvSource({
    "503, ServerBusy",
    "500, OperationTimedOut",
    "500, InternalError",
    "502, BadGateway",
    "504, GatewayTimeout",
    "429, TooManyRequests"
  })
  void aThrottledAppendFailsAsAThrottle(int status, String code) {
    DataLakeFileClient file = file(URL, request -> error(request, status, code));

    assertThatThrownBy(() -> write(pinned(file, null), AtomicOutputFile.Strategy.APPEND))
        .isInstanceOf(SupportsAtomicOperations.StorageThrottleException.class)
        .hasMessageContaining(status + " " + code)
        .cause()
        .isInstanceOf(BlobStorageException.class)
        .extracting(e -> ((BlobStorageException) e).getStatusCode())
        .isEqualTo(status);
  }

  @ParameterizedTest
  @CsvSource({"400, Md5Mismatch", "403, AuthorizationFailure", "501, NotImplemented"})
  void aRefusedAppendIsNoThrottle(int status, String code) {
    DataLakeFileClient file = file(URL, request -> error(request, status, code));

    assertThatThrownBy(() -> write(pinned(file, null), AtomicOutputFile.Strategy.APPEND))
        .isInstanceOf(BlobStorageException.class)
        .extracting(e -> ((BlobStorageException) e).getStatusCode())
        .isEqualTo(status);
  }

  /** A failed condition is a conflict, as it was. */
  @Test
  void aFailedAppendConditionIsAConflict() {
    DataLakeFileClient file =
        file(URL, request -> error(request, 412, "AppendPositionConditionNotMet"));

    assertThatThrownBy(() -> write(pinned(file, null), AtomicOutputFile.Strategy.APPEND))
        .isExactlyInstanceOf(SupportsAtomicOperations.AppendException.class);
  }

  /** An append from a snapshot that has seen no version looks the file up: a throttle too. */
  @Test
  void aThrottledLookupBeforeAnAppendFailsAsAThrottle() {
    DataLakeFileClient file = file(URL, request -> error(request, 500, "InternalError"));
    DataLakeStorageException busy = dataLakeError(503, "ServerBusy");
    when(file.getProperties()).thenThrow(busy);
    ADLSOutputFile out =
        new ADLSOutputFile(
            LOCATION,
            file,
            new AzureProperties(),
            -1L,
            new DataLakeRequestConditions().setIfNoneMatch("*"),
            MetricsContext.nullMetrics(),
            location -> file,
            true);

    assertThatThrownBy(() -> write(out, AtomicOutputFile.Strategy.APPEND))
        .isInstanceOf(SupportsAtomicOperations.StorageThrottleException.class)
        .cause()
        .isSameAs(busy);
  }

  /** A CAS stages its content in a temp blob first: a throttled staging leaves the target. */
  @Test
  void aThrottledStagingFailsAsAThrottle() {
    DataLakeFileClient file = file(URL, request -> error(request, 503, "ServerBusy"));
    DataLakeFileClient temp = file(TEMP_URL, request -> error(request, 503, "ServerBusy"));

    assertThatThrownBy(() -> write(pinned(file, temp), AtomicOutputFile.Strategy.CAS))
        .isInstanceOf(SupportsAtomicOperations.StorageThrottleException.class)
        .hasMessageContaining("503 ServerBusy")
        .cause()
        .isInstanceOf(BlobStorageException.class);
  }

  /**
   * The rename onto the target ends a CAS: a throttled one may have landed, as the throttle says.
   */
  @Test
  void aThrottledRenameFailsAsAThrottle() {
    DataLakeFileClient file = file(URL, request -> error(request, 500, "InternalError"));
    DataLakeFileClient temp = file(TEMP_URL, TestADLSOutputFileThrottle::staged);
    DataLakeStorageException busy = dataLakeError(503, "ServerBusy");
    when(temp.renameWithResponse(any(), any(), any(), any(), any(), any())).thenThrow(busy);

    assertThatThrownBy(() -> write(pinned(file, temp), AtomicOutputFile.Strategy.CAS))
        .isInstanceOf(SupportsAtomicOperations.StorageThrottleException.class)
        .hasMessageContaining("503 ServerBusy")
        .cause()
        .isSameAs(busy);
  }

  @Test
  void aRefusedRenameIsNoThrottle() {
    DataLakeFileClient file = file(URL, request -> error(request, 500, "InternalError"));
    DataLakeFileClient temp = file(TEMP_URL, TestADLSOutputFileThrottle::staged);
    DataLakeStorageException refused = dataLakeError(400, "InvalidRenameSourcePath");
    when(temp.renameWithResponse(any(), any(), any(), any(), any(), any())).thenThrow(refused);

    assertThatThrownBy(() -> write(pinned(file, temp), AtomicOutputFile.Strategy.CAS))
        .isSameAs(refused);
  }

  @Test
  void aFailedRenameConditionIsAConflict() {
    DataLakeFileClient file = file(URL, request -> error(request, 500, "InternalError"));
    DataLakeFileClient temp = file(TEMP_URL, TestADLSOutputFileThrottle::staged);
    when(temp.renameWithResponse(any(), any(), any(), any(), any(), any()))
        .thenThrow(dataLakeError(412, "ConditionNotMet"));

    assertThatThrownBy(() -> write(pinned(file, temp), AtomicOutputFile.Strategy.CAS))
        .isExactlyInstanceOf(SupportsAtomicOperations.CASException.class);
  }

  @Test
  void onlyRetryLaterStatusesAreThrottles() {
    assertThat(ADLSOutputFile.throttled(503)).isTrue();
    assertThat(ADLSOutputFile.throttled(500)).isTrue();
    assertThat(ADLSOutputFile.throttled(429)).isTrue();
    assertThat(ADLSOutputFile.throttled(400)).isFalse();
    assertThat(ADLSOutputFile.throttled(409)).isFalse();
    assertThat(ADLSOutputFile.throttled(412)).isFalse();
    assertThat(ADLSOutputFile.throttled(501)).isFalse();
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

  private static void write(AtomicOutputFile out, AtomicOutputFile.Strategy strategy)
      throws IOException {
    CAS token = out.prepare(() -> new ByteArrayInputStream(BYTES), strategy);
    out.writeAtomic(token, () -> new ByteArrayInputStream(BYTES));
  }

  /**
   * A Data Lake client whose Blob API requests reach the stub, which answers with {@code respond}.
   */
  private static DataLakeFileClient file(String url, Function<HttpRequest, HttpResponse> respond) {
    HttpPipeline pipeline =
        new HttpPipelineBuilder()
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
