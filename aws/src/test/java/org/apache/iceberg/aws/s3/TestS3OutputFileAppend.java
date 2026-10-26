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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

class TestS3OutputFileAppend {

  private static final byte[] BYTES = "appended".getBytes(StandardCharsets.UTF_8);

  /**
   * An append to a snapshot that carries its length and ETag is one PutObject, at that length and
   * conditioned on that ETag: no HEAD for the length. The snapshot it returns chains the next.
   */
  @Test
  void appendToASnapshotSendsOnePut() throws IOException {
    S3Client s3 = mock(S3Client.class);
    ArgumentCaptor<PutObjectRequest> puts = ArgumentCaptor.forClass(PutObjectRequest.class);
    when(s3.putObject(puts.capture(), any(RequestBody.class)))
        .thenReturn(PutObjectResponse.builder().eTag("\"second\"").build())
        .thenReturn(PutObjectResponse.builder().eTag("\"third\"").build());
    S3FileIO io = new S3FileIO(() -> s3);
    io.initialize(Map.of());
    InputFile snapshot =
        new S3InputFile(
            s3,
            null,
            new S3URI("s3://bucket--use1-az4--x-s3/key"),
            10L,
            new S3FileIOProperties(),
            MetricsContext.nullMetrics(),
            "\"first\"");

    InputFile next = append(io, snapshot);
    append(io, next);

    verify(s3, never()).headObject(any(HeadObjectRequest.class));
    assertThat(puts.getAllValues()).hasSize(2);
    assertThat(puts.getAllValues().get(0).writeOffsetBytes()).isEqualTo(10L);
    assertThat(puts.getAllValues().get(0).ifMatch()).isEqualTo("\"first\"");
    assertThat(puts.getAllValues().get(1).writeOffsetBytes()).isEqualTo(10L + BYTES.length);
    assertThat(puts.getAllValues().get(1).ifMatch()).isEqualTo("\"second\"");
  }

  private static InputFile append(S3FileIO io, InputFile snapshot) throws IOException {
    AtomicOutputFile out = io.newOutputFile(snapshot);
    CAS token =
        out.prepare(() -> new ByteArrayInputStream(BYTES), AtomicOutputFile.Strategy.APPEND);
    return out.writeAtomic(token, () -> new ByteArrayInputStream(BYTES));
  }
}
