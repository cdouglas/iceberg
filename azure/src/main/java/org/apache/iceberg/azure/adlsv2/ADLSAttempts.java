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

import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelineNextSyncPolicy;
import com.azure.core.http.HttpPipelinePosition;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.Context;
import java.util.concurrent.atomic.AtomicInteger;
import reactor.core.publisher.Mono;

/**
 * Counts the attempts the SDK sends of a request whose context carries a counter ({@link
 * #counting}). It sits after the SDK's retry policy ({@link HttpPipelinePosition#PER_RETRY}), so it
 * sees each attempt the retry policy sends. A conditional write's client error proves that nothing
 * landed only if the write was sent once: the storage SDK retries a 429, a 500, a 503, a timeout
 * and a lost connection, any of which may follow an attempt the service applied, and it reports
 * only the last attempt's error.
 */
final class ADLSAttempts implements HttpPipelinePolicy {
  private static final String KEY = ADLSAttempts.class.getName();

  /** A context in which each attempt of a request adds one to {@code attempts}. */
  static Context counting(AtomicInteger attempts) {
    return new Context(KEY, attempts);
  }

  @Override
  public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
    count(context);
    return next.process();
  }

  @Override
  public HttpResponse processSync(
      HttpPipelineCallContext context, HttpPipelineNextSyncPolicy next) {
    count(context);
    return next.processSync();
  }

  @Override
  public HttpPipelinePosition getPipelinePosition() {
    return HttpPipelinePosition.PER_RETRY;
  }

  private static void count(HttpPipelineCallContext context) {
    context.getData(KEY).ifPresent(attempts -> ((AtomicInteger) attempts).incrementAndGet());
  }
}
