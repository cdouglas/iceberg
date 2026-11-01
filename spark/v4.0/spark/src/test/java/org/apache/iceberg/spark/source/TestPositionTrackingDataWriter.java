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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.spark.PositionMappingCoordinator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Unit tests for PositionTrackingDataWriter. */
public class TestPositionTrackingDataWriter {

  private DataWriter<InternalRow> mockDelegate;
  private Table mockTable;
  private PositionMappingCoordinator coordinator;
  private UUID tableUuid;
  private String fileSetId;
  private StructType dsSchema;

  @SuppressWarnings("unchecked")
  @BeforeEach
  public void setup() {
    mockDelegate = mock(DataWriter.class);
    mockTable = mock(Table.class);
    coordinator = PositionMappingCoordinator.get();

    tableUuid = UUID.randomUUID();
    fileSetId = UUID.randomUUID().toString();
    when(mockTable.uuid()).thenReturn(tableUuid);

    // Schema with 3 data columns + 2 metadata columns (_file, _pos)
    dsSchema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("data", DataTypes.StringType, true, Metadata.empty()),
              new StructField("value", DataTypes.LongType, true, Metadata.empty()),
              new StructField("_file", DataTypes.StringType, false, Metadata.empty()),
              new StructField("_pos", DataTypes.LongType, false, Metadata.empty())
            });
  }

  @AfterEach
  public void cleanup() {
    coordinator.clearRewrite(mockTable, fileSetId);
  }

  private PositionTrackingDataWriter newWriter() {
    return new PositionTrackingDataWriter(mockDelegate, fileSetId, dsSchema);
  }

  private static DataFile dataFile(String path, long recordCount) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(path)
        .withFileSizeInBytes(1024)
        .withRecordCount(recordCount)
        .build();
  }

  /** Creates a TaskCommit for the given files, which hold the rows written in order. */
  private static SparkWrite.TaskCommit taskCommit(DataFile... files) {
    return new SparkWrite.TaskCommit(files);
  }

  private static InternalRow row(String sourceFile, long sourcePos) {
    InternalRow mockRow = mock(InternalRow.class);
    when(mockRow.getUTF8String(3)).thenReturn(UTF8String.fromString(sourceFile));
    when(mockRow.getLong(4)).thenReturn(sourcePos);
    return mockRow;
  }

  /** Records a task's commit message as the rewrite's commit does on the driver. */
  private Map<String, RewriteFileGroup.FilePositionMapping> recordOnDriver(
      WriterCommitMessage... messages) {
    PositionTrackingDataWriter.recordSourcePositions(mockTable, fileSetId, messages);
    return coordinator.fetchMappings(mockTable, fileSetId);
  }

  @Test
  public void testExtractMetadataAndPassFullRow() throws IOException {
    PositionTrackingDataWriter writer = newWriter();

    InternalRow mockRow = row("source.parquet", 42L);

    // Setup mock for delegate commit to return TaskCommit with target file
    when(mockDelegate.commit()).thenReturn(taskCommit(dataFile("target.parquet", 1)));

    writer.write(mockRow);

    // Verify delegate was called with the SAME row (no projection - delegate handles schema)
    ArgumentCaptor<InternalRow> rowCaptor = ArgumentCaptor.forClass(InternalRow.class);
    verify(mockDelegate, times(1)).write(rowCaptor.capture());
    assertThat(rowCaptor.getValue()).isSameAs(mockRow);

    Map<String, RewriteFileGroup.FilePositionMapping> mappings = recordOnDriver(writer.commit());
    assertThat(mappings).hasSize(1);
    RewriteFileGroup.FilePositionMapping.Run run = mappings.get("source.parquet").runs().get(0);
    assertThat(run.sourceOffset()).isEqualTo(42L);
    assertThat(run.targetOffset()).isEqualTo(0L);
    assertThat(run.length()).isEqualTo(1L);
    assertThat(run.targetFile()).isEqualTo("target.parquet");
  }

  @Test
  public void testCommitReturnsMappingsInsteadOfRecordingThem() throws Exception {
    PositionTrackingDataWriter writer = newWriter();
    when(mockDelegate.commit()).thenReturn(taskCommit(dataFile("target.parquet", 3)));
    for (long pos = 0; pos < 3; pos += 1) {
      writer.write(row("source.parquet", pos));
    }

    WriterCommitMessage message = writer.commit();

    // The task runs on an executor, whose JVM's coordinator the driver never reads
    assertThat(coordinator.fetchMappings(mockTable, fileSetId)).isEmpty();
    assertThat(message).isInstanceOf(PositionTrackingDataWriter.TrackedTaskCommit.class);

    // Spark serializes the message to send it to the driver
    WriterCommitMessage received = TestHelpers.roundTripSerialize(message);
    assertThat(((SparkWrite.TaskCommit) received).files())
        .extracting(DataFile::location)
        .containsExactly("target.parquet");

    Map<String, RewriteFileGroup.FilePositionMapping> mappings = recordOnDriver(received);
    assertThat(mappings).hasSize(1);
    RewriteFileGroup.FilePositionMapping.Run run = mappings.get("source.parquet").runs().get(0);
    assertThat(run.sourceOffset()).isEqualTo(0L);
    assertThat(run.targetOffset()).isEqualTo(0L);
    assertThat(run.length()).isEqualTo(3L);
    assertThat(run.targetFile()).isEqualTo("target.parquet");
  }

  @Test
  public void testMultipleWrites() throws IOException {
    PositionTrackingDataWriter writer = newWriter();

    // Setup mock for delegate commit to return TaskCommit with target file
    when(mockDelegate.commit()).thenReturn(taskCommit(dataFile("target.parquet", 3)));

    // Write 3 rows from same source with consecutive positions
    for (int i = 0; i < 3; i++) {
      writer.write(row("source.parquet", i));
    }

    // Verify delegate was called 3 times
    verify(mockDelegate, times(3)).write(any(InternalRow.class));

    WriterCommitMessage message = writer.commit();
    assertThat(((PositionTrackingDataWriter.TrackedTaskCommit) message).runCount()).isOne();

    // Verify mappings recorded correctly
    Map<String, RewriteFileGroup.FilePositionMapping> mappings = recordOnDriver(message);
    assertThat(mappings).hasSize(1);

    RewriteFileGroup.FilePositionMapping mapping = mappings.get("source.parquet");
    assertThat(mapping.runs()).hasSize(1);
    assertThat(mapping.runs().get(0).length()).isEqualTo(3L);
  }

  @Test
  public void testCommitResetsState() throws IOException {
    PositionTrackingDataWriter writer = newWriter();

    when(mockDelegate.commit())
        .thenReturn(taskCommit(dataFile("target1.parquet", 2)))
        .thenReturn(taskCommit(dataFile("target2.parquet", 1)));

    writer.write(row("source.parquet", 0L));
    writer.write(row("source.parquet", 1L));
    WriterCommitMessage first = writer.commit();

    // The next commit's positions start over in its own file
    writer.write(row("source.parquet", 2L));
    WriterCommitMessage second = writer.commit();

    verify(mockDelegate, times(2)).commit();

    List<RewriteFileGroup.FilePositionMapping.Run> runs =
        recordOnDriver(first, second).get("source.parquet").runs();
    assertThat(runs).hasSize(2);
    assertThat(runs.get(0).targetFile()).isEqualTo("target1.parquet");
    assertThat(runs.get(0).length()).isEqualTo(2L);
    assertThat(runs.get(1).sourceOffset()).isEqualTo(2L);
    assertThat(runs.get(1).targetOffset()).isEqualTo(0L);
    assertThat(runs.get(1).targetFile()).isEqualTo("target2.parquet");
  }

  @Test
  public void testCommitWithoutRowsReturnsTheDelegateMessage() throws IOException {
    PositionTrackingDataWriter writer = newWriter();
    SparkWrite.TaskCommit message = taskCommit();
    when(mockDelegate.commit()).thenReturn(message);

    assertThat(writer.commit()).isSameAs(message);
  }

  @Test
  public void testCommitFailsWithoutATaskCommit() throws IOException {
    PositionTrackingDataWriter writer = newWriter();
    when(mockDelegate.commit()).thenReturn(mock(WriterCommitMessage.class));
    writer.write(row("source.parquet", 0L));

    // Leaving the rows out would read as if they were deleted
    assertThatThrownBy(writer::commit)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("expected a TaskCommit");
  }

  @Test
  public void testCommitFailsIfTheFilesHoldOtherRows() throws IOException {
    PositionTrackingDataWriter writer = newWriter();
    when(mockDelegate.commit()).thenReturn(taskCommit(dataFile("target.parquet", 10)));
    writer.write(row("source.parquet", 0L));
    writer.write(row("source.parquet", 1L));

    assertThatThrownBy(writer::commit)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("wrote 2 rows, but the task's files hold 10");
  }

  @Test
  public void testAbortDelegates() throws IOException {
    PositionTrackingDataWriter writer = newWriter();

    writer.abort();

    verify(mockDelegate, times(1)).abort();
  }

  @Test
  public void testCloseDelegates() throws IOException {
    PositionTrackingDataWriter writer = newWriter();

    writer.close();

    verify(mockDelegate, times(1)).close();
  }

  @Test
  public void testWriteWithNullDataColumn() throws IOException {
    PositionTrackingDataWriter writer = newWriter();

    InternalRow mockRow = row("source.parquet", 0L);
    when(mockRow.isNullAt(1)).thenReturn(true); // Null data column

    writer.write(mockRow);

    // Verify delegate was called with the same row (no projection)
    ArgumentCaptor<InternalRow> rowCaptor = ArgumentCaptor.forClass(InternalRow.class);
    verify(mockDelegate, times(1)).write(rowCaptor.capture());
    assertThat(rowCaptor.getValue()).isSameAs(mockRow);
  }

  @Test
  public void testMultipleSourceFiles() throws IOException {
    PositionTrackingDataWriter writer = newWriter();

    // Setup mock for delegate commit to return TaskCommit with target file
    when(mockDelegate.commit()).thenReturn(taskCommit(dataFile("target.parquet", 30)));

    // Write rows from different source files
    String[] sourceFiles = {"sourceA.parquet", "sourceB.parquet", "sourceC.parquet"};

    for (String sourceFile : sourceFiles) {
      for (int i = 0; i < 10; i++) {
        writer.write(row(sourceFile, i));
      }
    }

    // Verify all three sources tracked
    Map<String, RewriteFileGroup.FilePositionMapping> mappings = recordOnDriver(writer.commit());
    assertThat(mappings).hasSize(3);
    assertThat(mappings.keySet())
        .containsExactlyInAnyOrder("sourceA.parquet", "sourceB.parquet", "sourceC.parquet");

    // Verify each has correct number of rows, after the rows of the files before it
    for (int i = 0; i < sourceFiles.length; i++) {
      RewriteFileGroup.FilePositionMapping mapping = mappings.get(sourceFiles[i]);
      assertThat(mapping.runs().get(0).length()).isEqualTo(10L);
      assertThat(mapping.runs().get(0).targetOffset()).isEqualTo(10L * i);
    }
  }

  @Test
  public void testOutputPositionIncrementsCorrectly() throws IOException {
    PositionTrackingDataWriter writer = newWriter();

    // Setup mock for delegate commit to return TaskCommit with target file
    when(mockDelegate.commit()).thenReturn(taskCommit(dataFile("target.parquet", 5)));

    // Write rows with non-consecutive source positions (simulating deletes)
    long[] sourcePositions = {0L, 1L, 5L, 6L, 10L}; // Gaps at 2-4, 7-9

    for (long sourcePosition : sourcePositions) {
      writer.write(row("source.parquet", sourcePosition));
    }

    // Verify mappings show gaps
    Map<String, RewriteFileGroup.FilePositionMapping> mappings = recordOnDriver(writer.commit());
    RewriteFileGroup.FilePositionMapping mapping = mappings.get("source.parquet");

    // One run per stretch of consecutive rows
    assertThat(mapping.runs()).hasSize(3);

    // Verify target positions are consecutive (0, 1, 2, 3, 4)
    long expectedTargetPos = 0L;
    for (RewriteFileGroup.FilePositionMapping.Run run : mapping.runs()) {
      assertThat(run.targetOffset()).isEqualTo(expectedTargetPos);
      expectedTargetPos += run.length();
    }
    assertThat(expectedTargetPos).isEqualTo(5L); // Total rows written
  }

  @Test
  public void testRunsSplitWhereTheWriterRolledToANewFile() throws IOException {
    PositionTrackingDataWriter writer = newWriter();

    // The writer rolled over after 10 rows, in the middle of source A
    when(mockDelegate.commit())
        .thenReturn(taskCommit(dataFile("target1.parquet", 10), dataFile("target2.parquet", 15)));
    for (long pos = 0; pos < 15; pos += 1) {
      writer.write(row("sourceA.parquet", pos));
    }

    for (long pos = 0; pos < 10; pos += 1) {
      writer.write(row("sourceB.parquet", pos));
    }

    Map<String, RewriteFileGroup.FilePositionMapping> mappings = recordOnDriver(writer.commit());

    List<RewriteFileGroup.FilePositionMapping.Run> runsOfA = mappings.get("sourceA.parquet").runs();
    assertThat(runsOfA).hasSize(2);
    assertThat(runsOfA.get(0).sourceOffset()).isEqualTo(0L);
    assertThat(runsOfA.get(0).targetOffset()).isEqualTo(0L);
    assertThat(runsOfA.get(0).length()).isEqualTo(10L);
    assertThat(runsOfA.get(0).targetFile()).isEqualTo("target1.parquet");
    assertThat(runsOfA.get(1).sourceOffset()).isEqualTo(10L);
    assertThat(runsOfA.get(1).targetOffset()).isEqualTo(0L);
    assertThat(runsOfA.get(1).length()).isEqualTo(5L);
    assertThat(runsOfA.get(1).targetFile()).isEqualTo("target2.parquet");

    List<RewriteFileGroup.FilePositionMapping.Run> runsOfB = mappings.get("sourceB.parquet").runs();
    assertThat(runsOfB).hasSize(1);
    assertThat(runsOfB.get(0).sourceOffset()).isEqualTo(0L);
    assertThat(runsOfB.get(0).targetOffset()).isEqualTo(5L);
    assertThat(runsOfB.get(0).length()).isEqualTo(10L);
    assertThat(runsOfB.get(0).targetFile()).isEqualTo("target2.parquet");
  }

  @Test
  public void testRunsSkipAnEmptyFile() throws IOException {
    PositionTrackingDataWriter writer = newWriter();
    when(mockDelegate.commit())
        .thenReturn(
            taskCommit(
                dataFile("target1.parquet", 2),
                dataFile("empty.parquet", 0),
                dataFile("target2.parquet", 2)));
    for (long pos = 0; pos < 4; pos += 1) {
      writer.write(row("source.parquet", pos));
    }

    List<RewriteFileGroup.FilePositionMapping.Run> runs =
        recordOnDriver(writer.commit()).get("source.parquet").runs();

    assertThat(runs)
        .extracting(run -> run.targetFile())
        .containsExactly("target1.parquet", "target2.parquet");
    assertThat(runs.get(1).sourceOffset()).isEqualTo(2L);
    assertThat(runs.get(1).targetOffset()).isEqualTo(0L);
  }
}
