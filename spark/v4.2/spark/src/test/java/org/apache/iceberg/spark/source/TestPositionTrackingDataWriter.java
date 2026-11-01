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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
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

  /** Creates a TaskCommit with the specified target file path for testing. */
  private SparkWrite.TaskCommit createTaskCommit(String targetFilePath) {
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(targetFilePath)
            .withFileSizeInBytes(1024)
            .withRecordCount(10)
            .build();
    return new SparkWrite.TaskCommit(new DataFile[] {dataFile});
  }

  @Test
  public void testExtractMetadataAndPassFullRow() throws IOException {
    PositionTrackingDataWriter writer =
        new PositionTrackingDataWriter(mockDelegate, mockTable, fileSetId, dsSchema);

    InternalRow mockRow = mock(InternalRow.class);
    when(mockRow.getUTF8String(3)).thenReturn(UTF8String.fromString("source.parquet"));
    when(mockRow.getLong(4)).thenReturn(42L);

    // Setup mock for delegate commit to return TaskCommit with target file
    SparkWrite.TaskCommit taskCommit = createTaskCommit("target.parquet");
    when(mockDelegate.commit()).thenReturn(taskCommit);

    writer.write(mockRow);

    // Verify delegate was called with the SAME row (no projection - delegate handles schema)
    ArgumentCaptor<InternalRow> rowCaptor = ArgumentCaptor.forClass(InternalRow.class);
    verify(mockDelegate, times(1)).write(rowCaptor.capture());
    assertThat(rowCaptor.getValue()).isSameAs(mockRow);

    // Mappings are buffered until commit - verify empty before commit
    Map<String, RewriteFileGroup.FilePositionMapping> mappingsBeforeCommit =
        coordinator.fetchMappings(mockTable, fileSetId);
    assertThat(mappingsBeforeCommit).isEmpty();

    // Commit to record mappings
    writer.commit();

    // Verify mapping was recorded after commit
    Map<String, RewriteFileGroup.FilePositionMapping> mappings =
        coordinator.fetchMappings(mockTable, fileSetId);
    assertThat(mappings).hasSize(1);
    assertThat(mappings.containsKey("source.parquet")).isTrue();
  }

  @Test
  public void testMultipleWrites() throws IOException {
    PositionTrackingDataWriter writer =
        new PositionTrackingDataWriter(mockDelegate, mockTable, fileSetId, dsSchema);

    // Setup mock for delegate commit to return TaskCommit with target file
    SparkWrite.TaskCommit taskCommit = createTaskCommit("target.parquet");
    when(mockDelegate.commit()).thenReturn(taskCommit);

    // Write 3 rows from same source with consecutive positions
    for (int i = 0; i < 3; i++) {
      InternalRow mockRow = mock(InternalRow.class);
      when(mockRow.getUTF8String(3)).thenReturn(UTF8String.fromString("source.parquet"));
      when(mockRow.getLong(4)).thenReturn((long) i);

      writer.write(mockRow);
    }

    // Verify delegate was called 3 times
    verify(mockDelegate, times(3)).write(any(InternalRow.class));

    // Commit to record mappings
    writer.commit();

    // Verify mappings recorded correctly
    Map<String, RewriteFileGroup.FilePositionMapping> mappings =
        coordinator.fetchMappings(mockTable, fileSetId);
    assertThat(mappings).hasSize(1);

    RewriteFileGroup.FilePositionMapping mapping = mappings.get("source.parquet");
    assertThat(mapping.runs()).hasSize(1);
    assertThat(mapping.runs().get(0).length()).isEqualTo(3L);
  }

  @Test
  public void testCommitResetsState() throws IOException {
    PositionTrackingDataWriter writer =
        new PositionTrackingDataWriter(mockDelegate, mockTable, fileSetId, dsSchema);

    WriterCommitMessage mockCommitMessage = mock(WriterCommitMessage.class);
    when(mockDelegate.commit()).thenReturn(mockCommitMessage);

    // Write some rows
    InternalRow mockRow = mock(InternalRow.class);
    when(mockRow.getUTF8String(3)).thenReturn(UTF8String.fromString("source.parquet"));
    when(mockRow.getLong(4)).thenReturn(0L);
    when(mockRow.isNullAt(any(Integer.class))).thenReturn(false);
    when(mockRow.get(any(Integer.class), any())).thenReturn(1);

    writer.write(mockRow);
    writer.write(mockRow);

    // Commit
    WriterCommitMessage result = writer.commit();
    assertThat(result).isEqualTo(mockCommitMessage);

    // Verify delegate commit was called
    verify(mockDelegate, times(1)).commit();
  }

  @Test
  public void testAbortDelegates() throws IOException {
    PositionTrackingDataWriter writer =
        new PositionTrackingDataWriter(mockDelegate, mockTable, fileSetId, dsSchema);

    writer.abort();

    verify(mockDelegate, times(1)).abort();
  }

  @Test
  public void testCloseDelegates() throws IOException {
    PositionTrackingDataWriter writer =
        new PositionTrackingDataWriter(mockDelegate, mockTable, fileSetId, dsSchema);

    writer.close();

    verify(mockDelegate, times(1)).close();
  }

  @Test
  public void testWriteWithNullDataColumn() throws IOException {
    PositionTrackingDataWriter writer =
        new PositionTrackingDataWriter(mockDelegate, mockTable, fileSetId, dsSchema);

    InternalRow mockRow = mock(InternalRow.class);
    when(mockRow.getUTF8String(3)).thenReturn(UTF8String.fromString("source.parquet"));
    when(mockRow.getLong(4)).thenReturn(0L);
    when(mockRow.isNullAt(1)).thenReturn(true); // Null data column

    writer.write(mockRow);

    // Verify delegate was called with the same row (no projection)
    ArgumentCaptor<InternalRow> rowCaptor = ArgumentCaptor.forClass(InternalRow.class);
    verify(mockDelegate, times(1)).write(rowCaptor.capture());
    assertThat(rowCaptor.getValue()).isSameAs(mockRow);
  }

  @Test
  public void testMultipleSourceFiles() throws IOException {
    PositionTrackingDataWriter writer =
        new PositionTrackingDataWriter(mockDelegate, mockTable, fileSetId, dsSchema);

    // Setup mock for delegate commit to return TaskCommit with target file
    SparkWrite.TaskCommit taskCommit = createTaskCommit("target.parquet");
    when(mockDelegate.commit()).thenReturn(taskCommit);

    // Write rows from different source files
    String[] sourceFiles = {"sourceA.parquet", "sourceB.parquet", "sourceC.parquet"};

    for (String sourceFile : sourceFiles) {
      for (int i = 0; i < 10; i++) {
        InternalRow mockRow = mock(InternalRow.class);
        when(mockRow.getUTF8String(3)).thenReturn(UTF8String.fromString(sourceFile));
        when(mockRow.getLong(4)).thenReturn((long) i);

        writer.write(mockRow);
      }
    }

    // Commit to record mappings
    writer.commit();

    // Verify all three sources tracked
    Map<String, RewriteFileGroup.FilePositionMapping> mappings =
        coordinator.fetchMappings(mockTable, fileSetId);
    assertThat(mappings).hasSize(3);
    assertThat(mappings.keySet())
        .containsExactlyInAnyOrder("sourceA.parquet", "sourceB.parquet", "sourceC.parquet");

    // Verify each has correct number of rows
    for (String sourceFile : sourceFiles) {
      RewriteFileGroup.FilePositionMapping mapping = mappings.get(sourceFile);
      assertThat(mapping.runs().get(0).length()).isEqualTo(10L);
    }
  }

  @Test
  public void testOutputPositionIncrementsCorrectly() throws IOException {
    PositionTrackingDataWriter writer =
        new PositionTrackingDataWriter(mockDelegate, mockTable, fileSetId, dsSchema);

    // Setup mock for delegate commit to return TaskCommit with target file
    SparkWrite.TaskCommit taskCommit = createTaskCommit("target.parquet");
    when(mockDelegate.commit()).thenReturn(taskCommit);

    // Write rows with non-consecutive source positions (simulating deletes)
    long[] sourcePositions = {0L, 1L, 5L, 6L, 10L}; // Gaps at 2-4, 7-9

    for (int i = 0; i < sourcePositions.length; i++) {
      InternalRow mockRow = mock(InternalRow.class);
      when(mockRow.getUTF8String(3)).thenReturn(UTF8String.fromString("source.parquet"));
      when(mockRow.getLong(4)).thenReturn(sourcePositions[i]);

      writer.write(mockRow);
    }

    // Commit to record mappings
    writer.commit();

    // Verify mappings show gaps
    Map<String, RewriteFileGroup.FilePositionMapping> mappings =
        coordinator.fetchMappings(mockTable, fileSetId);
    RewriteFileGroup.FilePositionMapping mapping = mappings.get("source.parquet");

    // Should have multiple runs due to gaps
    assertThat(mapping.runs().size()).isGreaterThan(1);

    // Verify target positions are consecutive (0, 1, 2, 3, 4)
    long expectedTargetPos = 0L;
    for (RewriteFileGroup.FilePositionMapping.Run run : mapping.runs()) {
      assertThat(run.targetOffset()).isEqualTo(expectedTargetPos);
      expectedTargetPos += run.length();
    }
    assertThat(expectedTargetPos).isEqualTo(5L); // Total rows written
  }
}
