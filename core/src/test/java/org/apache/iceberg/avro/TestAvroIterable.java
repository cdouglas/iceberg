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
package org.apache.iceberg.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class TestAvroIterable {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  @TempDir private Path temp;

  @Test
  void streamClosedOnIOException() throws Exception {
    InputFile inputFile = mock(InputFile.class);
    SeekableInputStream seekableInputStream = mock(SeekableInputStream.class);
    SeekableInput seekableInput = mock(SeekableInput.class);
    DatumReader<?> datumReader = mock(DatumReader.class);

    when(inputFile.newStream()).thenReturn(seekableInputStream);
    when(seekableInput.read(any(byte[].class), anyInt(), anyInt())).thenThrow(new IOException());

    try (MockedStatic<AvroIO> avroIo = mockStatic(AvroIO.class)) {
      avroIo
          .when(() -> AvroIO.stream(eq(seekableInputStream), any(LongSupplier.class)))
          .thenReturn(seekableInput);

      AvroIterable<?> iterable = new AvroIterable<>(inputFile, datumReader, null, null, false);

      Method method = iterable.getClass().getDeclaredMethod("newFileReader");
      method.setAccessible(true);
      assertThatThrownBy(() -> method.invoke(iterable))
          .hasCauseInstanceOf(RuntimeIOException.class);

      verify(seekableInput, atLeastOnce()).close();
    }
  }

  /**
   * A file read whole from its start, as a manifest list is, never asks its length: on an object
   * store whose input file does not know it, that is a HEAD request before the GET.
   */
  @Test
  void aFileReadWholeDoesNotAskItsLength() throws IOException {
    List<Record> expected = write(1_000);
    CountingInputFile in = new CountingInputFile(Files.localInput(temp.resolve("f.avro").toFile()));

    try (AvroIterable<Record> reader =
        Avro.read(in).createResolvingReader(PlannedDataReader::create).project(SCHEMA).build()) {
      assertThat(reader.getMetadata()).containsKey("iceberg.schema");
      assertThat(Lists.newArrayList(reader)).isEqualTo(expected);
    }

    assertThat(in.lengths.get()).as("length lookups").isZero();
  }

  /** A split still reads to its end, which it finds from the file's length. */
  @Test
  void aSplitAsksTheLengthItReadsTo() throws IOException {
    List<Record> expected = write(1_000);
    CountingInputFile in = new CountingInputFile(Files.localInput(temp.resolve("f.avro").toFile()));
    long length = in.file.getLength();

    try (AvroIterable<Record> reader =
        Avro.read(in)
            .createResolvingReader(PlannedDataReader::create)
            .split(0, length)
            .project(SCHEMA)
            .build()) {
      assertThat(Lists.newArrayList(reader)).isEqualTo(expected);
    }

    assertThat(in.lengths.get()).as("length lookups").isPositive();
  }

  /** An empty file is not an Avro file, as when the length was checked first. */
  @Test
  void anEmptyFileIsNotAnAvroFile() throws IOException {
    OutputFile out = Files.localOutput(temp.resolve("empty.avro").toFile());
    out.create().close();
    CountingInputFile in = new CountingInputFile(out.toInputFile());

    try (AvroIterable<Record> reader =
        Avro.read(in).createResolvingReader(PlannedDataReader::create).project(SCHEMA).build()) {
      assertThatThrownBy(reader::iterator)
          .isInstanceOf(RuntimeIOException.class)
          .hasMessageContaining("Failed to open file");
    }
  }

  private List<Record> write(int count) throws IOException {
    List<Record> records = Lists.newArrayList();
    OutputFile out = Files.localOutput(temp.resolve("f.avro").toFile());
    try (FileAppender<Record> writer =
        Avro.write(out).createWriterFunc(DataWriter::create).schema(SCHEMA).build()) {
      Record record = GenericRecord.create(SCHEMA);
      for (long i = 0; i < count; i += 1) {
        Record next = record.copy("id", i, "data", "row-" + i);
        records.add(next);
        writer.add(next);
      }
    }

    return records;
  }

  /** An input file that counts its length lookups. */
  private static class CountingInputFile implements InputFile {
    private final InputFile file;
    private final AtomicInteger lengths = new AtomicInteger();

    CountingInputFile(InputFile file) {
      this.file = file;
    }

    @Override
    public long getLength() {
      lengths.incrementAndGet();
      return file.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return file.newStream();
    }

    @Override
    public String location() {
      return file.location();
    }

    @Override
    public boolean exists() {
      return file.exists();
    }
  }
}
