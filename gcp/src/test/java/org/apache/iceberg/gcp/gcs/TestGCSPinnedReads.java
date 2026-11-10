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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Reads of one generation (SupportsAtomicOperations): a file that looked the object up reads the
 * generation the lookup found, by if-generation-match, even through a stream made before it.
 */
class TestGCSPinnedReads {

  private static final BlobId ID = BlobId.fromGsUtilUri("gs://bucket/catalog");

  private Storage storage;
  private ReadChannel channel;
  private ArgumentCaptor<BlobSourceOption> options;

  @BeforeEach
  void before() throws IOException {
    storage = mock(Storage.class);
    channel = mock(ReadChannel.class);
    options = ArgumentCaptor.forClass(BlobSourceOption.class);
    Blob blob = mock(Blob.class);
    when(blob.getGeneration()).thenReturn(7L);
    when(blob.getSize()).thenReturn(5L);
    when(storage.get(ID)).thenReturn(blob);
    when(storage.reader(eq(ID), options.capture())).thenReturn(channel);
    when(channel.read(any(ByteBuffer.class))).thenReturn(1);
  }

  private GCSInputFile file() {
    return new GCSInputFile(
        storage, null, ID, null, new GCPProperties(), MetricsContext.nullMetrics());
  }

  @Test
  void aStreamMadeBeforeTheLookupAsksForTheGenerationTheLookupFound() throws IOException {
    GCSInputFile file = file();
    try (SeekableInputStream in = file.newStream()) {
      verify(storage, never()).reader(eq(ID), any(BlobSourceOption[].class));
      assertThat(file.getLength()).isEqualTo(5);
      in.read();
      ((RangeReadable) in).readFully(0, new byte[1]);
    }

    List<BlobSourceOption> sent = options.getAllValues();
    assertThat(sent).hasSize(2).allMatch(BlobSourceOption.generationMatch(7L)::equals);
  }

  @Test
  void aFileThatHasSeenNoGenerationReadsTheLiveOne() throws IOException {
    try (SeekableInputStream in = file().newStream()) {
      in.read();
    }

    assertThat(options.getAllValues()).isEmpty();
  }

  @Test
  void aReadOfAReplacedGenerationFailsAsStale() throws IOException {
    when(channel.read(any(ByteBuffer.class)))
        .thenThrow(new IOException(new StorageException(412, "conditionNotMet")));
    GCSInputFile file = file();
    file.getLength();
    try (SeekableInputStream in = file.newStream()) {
      assertThatThrownBy(in::read).isInstanceOf(SupportsAtomicOperations.StaleReadException.class);
      assertThatThrownBy(() -> in.read(new byte[2], 0, 2))
          .isInstanceOf(SupportsAtomicOperations.StaleReadException.class);
      assertThatThrownBy(() -> ((RangeReadable) in).readFully(0, new byte[2]))
          .isInstanceOf(SupportsAtomicOperations.StaleReadException.class);
    }
  }

  /** A read that the library retries asks for the generation by number: gone, it is a 404. */
  @Test
  void aGenerationGoneMidReadFailsAsStale() throws IOException {
    when(channel.read(any(ByteBuffer.class)))
        .thenThrow(new IOException(new StorageException(404, "notFound")));
    GCSInputFile file = file();
    file.getLength();
    try (SeekableInputStream in = file.newStream()) {
      assertThatThrownBy(in::read).isInstanceOf(SupportsAtomicOperations.StaleReadException.class);
    }
  }
}
