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

import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.NoCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A conditional write that GCS refuses with a client error the same request gets again fails as
 * {@link SupportsAtomicOperations.StorageRefusedException}: nothing was written. An error that does
 * not prove that (a conflict, a 404, a timeout, a rate limit, or a client error that ends the
 * client library's retries of the write, which an earlier attempt may have landed before) keeps its
 * mapping, or propagates as it is. Writes go through the client library to a stub of the service,
 * which answers each request as a test says.
 */
class TestGCSOutputFileRefused {

  private static final BlobId TARGET = BlobId.of("bucket", "dir/file");
  private static final BlobId PINNED = BlobId.of("bucket", "dir/file", 5L);
  private static final byte[] BYTES = "written".getBytes(StandardCharsets.UTF_8);

  @ParameterizedTest
  @ValueSource(ints = {400, 401, 403, 405, 411, 413, 501})
  void aClientErrorOnTheOnlyAttemptIsARefusal(int status) {
    StorageException error = new StorageException(status, "refused", "forbidden", null);

    assertThat(GCSOutputFile.translate(error))
        .isExactlyInstanceOf(SupportsAtomicOperations.StorageRefusedException.class)
        .hasMessageContaining(String.valueOf(status))
        .cause()
        .isSameAs(error);
  }

  /**
   * 404, 408, 409 and 499 prove nothing; 412 is a conflict and 429 and the other 5xx throttles, as
   * they were.
   */
  @ParameterizedTest
  @ValueSource(ints = {404, 408, 409, 499})
  void anErrorThatProvesNothingIsNoRefusal(int status) {
    StorageException error = new StorageException(status, "error");

    assertThat(GCSOutputFile.translate(error)).isSameAs(error);
  }

  @Test
  void conflictsAndThrottlesKeepTheirMapping() {
    assertThat(GCSOutputFile.translate(new StorageException(412, "modified")))
        .isExactlyInstanceOf(SupportsAtomicOperations.StorageInvariantException.class);
    assertThat(List.of(429, 500, 502, 503, 504))
        .allSatisfy(
            status ->
                assertThat(GCSOutputFile.translate(new StorageException(status, "busy")))
                    .isExactlyInstanceOf(SupportsAtomicOperations.StorageThrottleException.class));
  }

  /** A 403 that is a rate limit is transient: no refusal. */
  @ParameterizedTest
  @ValueSource(strings = {"rateLimitExceeded", "userRateLimitExceeded"})
  void aRateLimitIsNoRefusal(String reason) {
    StorageException error = new StorageException(403, "slow down", reason, null);

    assertThat(GCSOutputFile.translate(error)).isSameAs(error);
  }

  /**
   * A client error that ends the client library's retries carries the earlier attempts' failures as
   * suppressed exceptions: an earlier attempt may have landed, so it is no refusal.
   */
  @Test
  void aClientErrorAfterEarlierAttemptsIsNoRefusal() {
    StorageException error = new StorageException(403, "refused", "forbidden", null);
    error.addSuppressed(new StorageException(503, "busy"));

    assertThat(GCSOutputFile.translate(error)).isSameAs(error);
  }

  /**
   * Through the client library: a 403 on the write's only attempt is a refusal, and so is one on
   * the zonal buckets' XML PUT, which sends once.
   */
  @Test
  void aWriteRefusedOnItsOnlyAttemptIsRefused() {
    StubTransport http = new StubTransport().answer(403);
    Storage storage = storage(http);

    assertThatThrownBy(() -> write(output(storage, null)))
        .isExactlyInstanceOf(SupportsAtomicOperations.StorageRefusedException.class)
        .hasMessageContaining("403")
        .cause()
        .isInstanceOf(StorageException.class);
    assertThat(http.requests).isEqualTo(1);

    http.answer(403);
    assertThatThrownBy(
            () -> write(output(storage, new GCSXmlPutWriter(storage, new GCPProperties()))))
        .isExactlyInstanceOf(SupportsAtomicOperations.StorageRefusedException.class);
    assertThat(http.requests).isEqualTo(2);
  }

  /**
   * Through the client library: a 503, which it retries (the write is conditional, so idempotent),
   * then a 403. The first attempt may have landed (a 5xx can follow a write GCS applied), so the
   * 403 that ends the retries is no refusal.
   */
  @Test
  void aClientErrorAfterTheLibrarysRetriesIsNoRefusal() {
    StubTransport http = new StubTransport().answer(503).answer(403);

    assertThatThrownBy(() -> write(output(storage(http), null)))
        .isExactlyInstanceOf(StorageException.class)
        .extracting(e -> ((StorageException) e).getCode())
        .isEqualTo(403);
    assertThat(http.requests).isEqualTo(2);
  }

  private static GCSOutputFile output(Storage storage, GCSXmlPutWriter zonal) {
    return GCSOutputFile.replacing(
        TARGET,
        PINNED,
        storage,
        null,
        new GCPProperties(),
        MetricsContext.nullMetrics(),
        zonal,
        null);
  }

  private static void write(AtomicOutputFile out) throws IOException {
    CAS token = out.prepare(() -> new ByteArrayInputStream(BYTES), AtomicOutputFile.Strategy.CAS);
    out.writeAtomic(token, () -> new ByteArrayInputStream(BYTES));
  }

  /** A client of the stub, retrying three times without a wait. */
  private static Storage storage(StubTransport http) {
    return StorageOptions.newBuilder()
        .setProjectId("project")
        .setCredentials(NoCredentials.getInstance())
        .setHost("http://localhost:9")
        .setTransportOptions(
            HttpTransportOptions.newBuilder().setHttpTransportFactory(() -> http).build())
        .setRetrySettings(
            RetrySettings.newBuilder()
                .setMaxAttempts(3)
                .setInitialRetryDelayDuration(Duration.ofMillis(1))
                .setMaxRetryDelayDuration(Duration.ofMillis(1))
                .setRetryDelayMultiplier(1.0)
                .setTotalTimeoutDuration(Duration.ofSeconds(30))
                .build())
        .build()
        .getService();
  }

  /** An HTTP transport that answers each request with the next error queued, as GCS would. */
  private static final class StubTransport extends MockHttpTransport {
    private final Deque<Integer> answers = new ArrayDeque<>();
    private int requests = 0;

    StubTransport answer(int status) {
      answers.add(status);
      return this;
    }

    @Override
    public LowLevelHttpRequest buildRequest(String method, String url) {
      return new MockLowLevelHttpRequest(url) {
        @Override
        public LowLevelHttpResponse execute() {
          requests += 1;
          int status = answers.remove();
          return new MockLowLevelHttpResponse()
              .setStatusCode(status)
              .setContentType("application/json")
              .setContent(
                  String.format(
                      Locale.ROOT,
                      "{\"error\":{\"code\":%d,\"message\":\"refused\","
                          + "\"errors\":[{\"reason\":\"%s\",\"message\":\"refused\"}]}}",
                      status,
                      status == 503 ? "backendError" : "forbidden"));
        }
      };
    }
  }
}
