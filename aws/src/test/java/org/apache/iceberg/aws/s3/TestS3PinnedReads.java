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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Reads of one version (SupportsAtomicOperations): a file reads the version a lookup found or its
 * first GET found, and every later request asks for it with If-Match.
 */
class TestS3PinnedReads {

  private static final String LOCATION = "s3://bucket/key";
  private static final byte[] BYTES = "0123456789".getBytes(StandardCharsets.UTF_8);

  private S3Client s3;
  private ArgumentCaptor<GetObjectRequest> gets;
  private S3FileIO io;

  @BeforeEach
  void before() {
    s3 = mock(S3Client.class);
    gets = ArgumentCaptor.forClass(GetObjectRequest.class);
    io = new S3FileIO(() -> s3);
    io.initialize(Map.of());
  }

  /** A GET of the object's bytes from {@code range}'s start, as version {@code etag}. */
  private static ResponseInputStream<GetObjectResponse> found(String etag, String range) {
    long start = Long.parseLong(range.substring("bytes=".length(), range.indexOf('-')));
    String end = range.substring(range.indexOf('-') + 1);
    long last = end.isEmpty() ? BYTES.length - 1 : Long.parseLong(end);
    byte[] body = Arrays.copyOfRange(BYTES, (int) start, (int) last + 1);
    GetObjectResponse response =
        GetObjectResponse.builder()
            .eTag(etag)
            .contentLength((long) body.length)
            .contentRange("bytes " + start + "-" + last + "/" + BYTES.length)
            .build();
    return new ResponseInputStream<>(
        response, AbortableInputStream.create(new ByteArrayInputStream(body)));
  }

  private void serve(String etag) {
    when(s3.getObject(gets.capture(), any(ResponseTransformer.class)))
        .thenAnswer(call -> found(etag, ((GetObjectRequest) call.getArgument(0)).range()));
  }

  private static S3Exception preconditionFailed() {
    return (S3Exception)
        S3Exception.builder().statusCode(412).message("PreconditionFailed").build();
  }

  @Test
  void aStreamsLaterRequestsAskForTheVersionItsFirstFound() throws IOException {
    serve("\"e1\"");
    InputFile file = io.newInputFile(LOCATION);
    try (SeekableInputStream in = file.newStream()) {
      assertThat(in.read()).isEqualTo('0');
      in.seek(0); // backward: a new request
      assertThat(in.read()).isEqualTo('0');
      byte[] middle = new byte[3];
      ((org.apache.iceberg.io.RangeReadable) in).readFully(4, middle);
      assertThat(middle).isEqualTo("456".getBytes(StandardCharsets.UTF_8));
    }

    List<GetObjectRequest> sent = gets.getAllValues();
    assertThat(sent).hasSize(3);
    assertThat(sent.get(0).ifMatch()).as("the first request asks for no version").isNull();
    assertThat(sent.subList(1, 3)).allSatisfy(get -> assertThat(get.ifMatch()).isEqualTo("\"e1\""));
  }

  /**
   * A file whose first request was a GET has seen the version it found: its length is that
   * version's, with no HEAD, and a CAS from it replaces only that version.
   */
  @Test
  void aFilesFirstReadChoosesItsVersionAndLength() throws IOException {
    serve("\"e1\"");
    ArgumentCaptor<PutObjectRequest> puts = ArgumentCaptor.forClass(PutObjectRequest.class);
    when(s3.putObject(puts.capture(), any(RequestBody.class)))
        .thenReturn(PutObjectResponse.builder().eTag("\"e2\"").build());
    InputFile file = io.newInputFile(LOCATION);
    try (SeekableInputStream in = file.newStream()) {
      assertThat(in.read()).isEqualTo('0');
    }

    assertThat(file.getLength()).isEqualTo(BYTES.length);
    verify(s3, never()).headObject(any(HeadObjectRequest.class));
    AtomicOutputFile out = io.newOutputFile(file);
    out.writeAtomic(
        out.prepare(() -> new ByteArrayInputStream(BYTES), AtomicOutputFile.Strategy.CAS),
        () -> new ByteArrayInputStream(BYTES));
    assertThat(puts.getValue().ifMatch()).isEqualTo("\"e1\"");
  }

  /**
   * A file whose first request is a GET from an offset to the end has seen the version it found and
   * the object's whole length, from the response's {@code Content-Range}: one request, no HEAD.
   */
  @Test
  void aFilesFirstRangedReadChoosesItsVersionAndTheObjectsLength() throws IOException {
    serve("\"e1\"");
    InputFile file = io.newInputFile(LOCATION);
    byte[] rest;
    try (SeekableInputStream in = file.newStream()) {
      in.seek(4);
      rest = in.readAllBytes();
    }

    assertThat(rest).isEqualTo("456789".getBytes(StandardCharsets.UTF_8));
    assertThat(file.getLength()).isEqualTo(BYTES.length);
    verify(s3, never()).headObject(any(HeadObjectRequest.class));
    assertThat(gets.getValue().range()).isEqualTo("bytes=4-");
    assertThat(gets.getValue().ifMatch()).isNull();
    assertThat(io.readsRecordVersion()).isTrue();
  }

  /**
   * A GET from the object's end or past it (S3 answers 416) is the stream's end, not an error; a
   * read from inside the object after it reads the object.
   */
  @Test
  void aReadFromTheEndOrPastItIsTheStreamsEnd() throws IOException {
    when(s3.getObject(gets.capture(), any(ResponseTransformer.class)))
        .thenAnswer(
            call -> {
              String range = ((GetObjectRequest) call.getArgument(0)).range();
              long start = Long.parseLong(range.substring("bytes=".length(), range.indexOf('-')));
              if (start >= BYTES.length) {
                throw S3Exception.builder().statusCode(416).message("InvalidRange").build();
              }
              return found("\"e1\"", range);
            });
    InputFile file = io.newInputFile(LOCATION);
    try (SeekableInputStream in = file.newStream()) {
      in.seek(BYTES.length);
      assertThat(in.read()).isEqualTo(-1);
      in.seek(BYTES.length + 20);
      assertThat(in.read(new byte[4], 0, 4)).isEqualTo(-1);
      in.seek(8);
      assertThat(in.readAllBytes()).isEqualTo("89".getBytes(StandardCharsets.UTF_8));
    }
  }

  /** A file that looked the object up reads the version the HEAD found. */
  @Test
  void aFileThatLookedReadsTheVersionTheLookupFound() throws IOException {
    serve("\"e1\"");
    when(s3.headObject(any(HeadObjectRequest.class)))
        .thenReturn(
            HeadObjectResponse.builder().eTag("\"e1\"").contentLength((long) BYTES.length).build());
    InputFile file = io.newInputFile(LOCATION);
    assertThat(file.getLength()).isEqualTo(BYTES.length);
    try (SeekableInputStream in = file.newStream()) {
      assertThat(in.read()).isEqualTo('0');
    }

    assertThat(gets.getValue().ifMatch()).isEqualTo("\"e1\"");
  }

  /** A request for a version that a write replaced or extended fails as stale. */
  @Test
  void aReadOfAReplacedVersionFailsAsStale() throws IOException {
    when(s3.headObject(any(HeadObjectRequest.class)))
        .thenReturn(
            HeadObjectResponse.builder().eTag("\"e1\"").contentLength((long) BYTES.length).build());
    when(s3.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
        .thenThrow(preconditionFailed());
    InputFile file = io.newInputFile(LOCATION);
    file.getLength();

    try (SeekableInputStream in = file.newStream()) {
      assertThatThrownBy(in::read).isInstanceOf(SupportsAtomicOperations.StaleReadException.class);
      assertThatThrownBy(() -> ((org.apache.iceberg.io.RangeReadable) in).readFully(0, new byte[2]))
          .isInstanceOf(SupportsAtomicOperations.StaleReadException.class);
    }
  }

  /**
   * A lookup through a file that a write returned asks for the version it wrote; if a later write
   * replaced it, the file does not exist, as a GCS generation's lookup finds.
   */
  @Test
  void aLookupAsksForTheVersionTheFileReads() {
    ArgumentCaptor<HeadObjectRequest> heads = ArgumentCaptor.forClass(HeadObjectRequest.class);
    when(s3.headObject(heads.capture())).thenThrow(preconditionFailed());
    S3InputFile written =
        new S3InputFile(
            s3, null, new S3URI(LOCATION), null, new S3FileIOProperties(), null, "\"e1\"");

    assertThat(written.exists()).isFalse();
    assertThat(heads.getValue().ifMatch()).isEqualTo("\"e1\"");
    assertThatThrownBy(written::getLength)
        .isInstanceOf(SupportsAtomicOperations.StaleReadException.class);
  }

  @Test
  void anObjectsLengthComesFromTheResponsesRange() {
    assertThat(
            S3InputFile.objectLength(
                GetObjectResponse.builder().contentRange("bytes 4-9/10").contentLength(6L).build()))
        .isEqualTo(10L);
    assertThat(S3InputFile.objectLength(GetObjectResponse.builder().contentLength(6L).build()))
        .isEqualTo(6L);
    assertThat(
            S3InputFile.objectLength(
                GetObjectResponse.builder().contentRange("bytes 4-9/*").contentLength(6L).build()))
        .isNull();
  }
}
