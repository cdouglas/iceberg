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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.InputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/** Atomic writes from a snapshot that has seen no version, and from one that looked. */
class TestS3UnseenSnapshot {

  private static final String LOCATION = "s3://bucket--use1-az4--x-s3/key";
  private static final byte[] BYTES = "written".getBytes(StandardCharsets.UTF_8);

  private S3Client s3;
  private ArgumentCaptor<PutObjectRequest> puts;
  private S3FileIO io;

  @BeforeEach
  void before() {
    s3 = mock(S3Client.class);
    puts = ArgumentCaptor.forClass(PutObjectRequest.class);
    when(s3.putObject(puts.capture(), any(RequestBody.class)))
        .thenReturn(PutObjectResponse.builder().eTag("\"written\"").build());
    when(s3.headObject(any(HeadObjectRequest.class)))
        .thenReturn(HeadObjectResponse.builder().eTag("\"seen\"").contentLength(10L).build());
    io = new S3FileIO(() -> s3);
    io.initialize(Map.of());
  }

  /**
   * A CAS from a snapshot that has seen no version assumes the object is absent: one PutObject with
   * {@code If-None-Match: *}, and no HEAD to find out.
   */
  @Test
  void aCasFromAnUnseenSnapshotCreatesWithoutAHead() throws IOException {
    InputFile created = write(io.newInputFile(LOCATION), AtomicOutputFile.Strategy.CAS);

    verify(s3, never()).headObject(any(HeadObjectRequest.class));
    assertThat(puts.getValue().ifNoneMatch()).isEqualTo("*");
    assertThat(puts.getValue().ifMatch()).isNull();
    assertThat(((S3InputFile) created).etag()).isEqualTo("\"written\"");
  }

  /** A CAS from a snapshot that looked replaces the version it saw, with no second HEAD. */
  @Test
  void aCasFromASnapshotThatLookedReplacesWhatItSaw() throws IOException {
    InputFile looked = io.newInputFile(LOCATION);
    assertThat(looked.getLength()).isEqualTo(10L);

    write(looked, AtomicOutputFile.Strategy.CAS);

    verify(s3, times(1)).headObject(any(HeadObjectRequest.class));
    assertThat(puts.getValue().ifMatch()).isEqualTo("\"seen\"");
    assertThat(puts.getValue().ifNoneMatch()).isNull();
  }

  /**
   * An append from a snapshot that has seen no version extends the object's current one: a HEAD
   * when it writes, then one PutObject at the snapshot's length, conditioned on that ETag.
   */
  @Test
  void anAppendFromAnUnseenSnapshotLooksTheObjectUp() throws IOException {
    write(io.newInputFile(LOCATION, 10L), AtomicOutputFile.Strategy.APPEND);

    verify(s3, times(1)).headObject(any(HeadObjectRequest.class));
    assertThat(puts.getValue().ifMatch()).isEqualTo("\"seen\"");
    assertThat(puts.getValue().writeOffsetBytes()).isEqualTo(10L);
  }

  private InputFile write(InputFile snapshot, AtomicOutputFile.Strategy strategy)
      throws IOException {
    AtomicOutputFile out = io.newOutputFile(snapshot);
    CAS token = out.prepare(() -> new ByteArrayInputStream(BYTES), strategy);
    return out.writeAtomic(token, () -> new ByteArrayInputStream(BYTES));
  }
}
