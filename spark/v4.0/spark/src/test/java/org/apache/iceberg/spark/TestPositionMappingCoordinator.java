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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for PositionMappingCoordinator. */
public class TestPositionMappingCoordinator {

  private PositionMappingCoordinator coordinator;
  private Table mockTable;
  private UUID tableUuid;
  private String fileSetId;

  @BeforeEach
  public void setup() {
    coordinator = PositionMappingCoordinator.get();
    mockTable = mock(Table.class);
    tableUuid = UUID.randomUUID();
    fileSetId = UUID.randomUUID().toString();

    when(mockTable.uuid()).thenReturn(tableUuid);
  }

  @AfterEach
  public void cleanup() {
    // Clean up any remaining state
    coordinator.clearRewrite(mockTable, fileSetId);
  }

  @Test
  public void testSimpleSequentialMapping() {
    // Record sequential positions without gaps
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 0L, "target1.parquet", 0L);
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 1L, "target1.parquet", 1L);
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 2L, "target1.parquet", 2L);
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 3L, "target1.parquet", 3L);

    Map<String, RewriteFileGroup.FilePositionMapping> mappings =
        coordinator.fetchMappings(mockTable, fileSetId);

    assertThat(mappings).hasSize(1);
    RewriteFileGroup.FilePositionMapping mapping = mappings.get("sourceA.parquet");
    assertThat(mapping.sourceFile()).isEqualTo("sourceA.parquet");
    assertThat(mapping.targetFile()).isEqualTo("target1.parquet");
    assertThat(mapping.runs()).hasSize(1);

    RewriteFileGroup.FilePositionMapping.Run run = mapping.runs().get(0);
    assertThat(run.sourceOffset()).isEqualTo(0L);
    assertThat(run.targetOffset()).isEqualTo(0L);
    assertThat(run.length()).isEqualTo(4L);
  }

  @Test
  public void testMappingWithGapsFromDeletes() {
    // Record positions with gaps representing deleted rows
    // Source positions: 0, 1, 3, 4 (position 2 deleted)
    // Target positions: 0, 1, 2, 3 (consecutive)
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 0L, "target1.parquet", 0L);
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 1L, "target1.parquet", 1L);
    // Gap at source position 2 (deleted)
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 3L, "target1.parquet", 2L);
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 4L, "target1.parquet", 3L);

    Map<String, RewriteFileGroup.FilePositionMapping> mappings =
        coordinator.fetchMappings(mockTable, fileSetId);

    assertThat(mappings).hasSize(1);
    RewriteFileGroup.FilePositionMapping mapping = mappings.get("sourceA.parquet");
    assertThat(mapping.runs()).hasSize(2);

    // First run: positions 0-1
    RewriteFileGroup.FilePositionMapping.Run run1 = mapping.runs().get(0);
    assertThat(run1.sourceOffset()).isEqualTo(0L);
    assertThat(run1.targetOffset()).isEqualTo(0L);
    assertThat(run1.length()).isEqualTo(2L);

    // Second run: positions 3-4
    RewriteFileGroup.FilePositionMapping.Run run2 = mapping.runs().get(1);
    assertThat(run2.sourceOffset()).isEqualTo(3L);
    assertThat(run2.targetOffset()).isEqualTo(2L);
    assertThat(run2.length()).isEqualTo(2L);
  }

  @Test
  public void testMappingWithMultipleGaps() {
    // Source positions: 0, 2, 5, 6, 8 (gaps at 1, 3-4, 7)
    // Target positions: 0, 1, 2, 3, 4
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 0L, "target1.parquet", 0L);
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 2L, "target1.parquet", 1L);
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 5L, "target1.parquet", 2L);
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 6L, "target1.parquet", 3L);
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 8L, "target1.parquet", 4L);

    Map<String, RewriteFileGroup.FilePositionMapping> mappings =
        coordinator.fetchMappings(mockTable, fileSetId);

    assertThat(mappings).hasSize(1);
    RewriteFileGroup.FilePositionMapping mapping = mappings.get("sourceA.parquet");
    assertThat(mapping.runs()).hasSize(4);

    assertThat(mapping.runs().get(0))
        .satisfies(
            run -> {
              assertThat(run.sourceOffset()).isEqualTo(0L);
              assertThat(run.targetOffset()).isEqualTo(0L);
              assertThat(run.length()).isEqualTo(1L);
            });

    assertThat(mapping.runs().get(1))
        .satisfies(
            run -> {
              assertThat(run.sourceOffset()).isEqualTo(2L);
              assertThat(run.targetOffset()).isEqualTo(1L);
              assertThat(run.length()).isEqualTo(1L);
            });

    assertThat(mapping.runs().get(2))
        .satisfies(
            run -> {
              assertThat(run.sourceOffset()).isEqualTo(5L);
              assertThat(run.targetOffset()).isEqualTo(2L);
              assertThat(run.length()).isEqualTo(2L);
            });

    assertThat(mapping.runs().get(3))
        .satisfies(
            run -> {
              assertThat(run.sourceOffset()).isEqualTo(8L);
              assertThat(run.targetOffset()).isEqualTo(4L);
              assertThat(run.length()).isEqualTo(1L);
            });
  }

  @Test
  public void testMultipleSourceFilesToSingleTarget() {
    // Multiple source files compacted into one target
    // SourceA: 100 rows -> target positions 0-99
    for (int i = 0; i < 100; i++) {
      coordinator.recordMapping(
          mockTable, fileSetId, "sourceA.parquet", (long) i, "target1.parquet", (long) i);
    }

    // SourceB: 50 rows -> target positions 100-149
    for (int i = 0; i < 50; i++) {
      coordinator.recordMapping(
          mockTable, fileSetId, "sourceB.parquet", (long) i, "target1.parquet", (long) (100 + i));
    }

    // SourceC: 75 rows -> target positions 150-224
    for (int i = 0; i < 75; i++) {
      coordinator.recordMapping(
          mockTable, fileSetId, "sourceC.parquet", (long) i, "target1.parquet", (long) (150 + i));
    }

    Map<String, RewriteFileGroup.FilePositionMapping> mappings =
        coordinator.fetchMappings(mockTable, fileSetId);

    assertThat(mappings).hasSize(3);

    // Verify sourceA mapping
    RewriteFileGroup.FilePositionMapping mappingA = mappings.get("sourceA.parquet");
    assertThat(mappingA.targetFile()).isEqualTo("target1.parquet");
    assertThat(mappingA.runs()).hasSize(1);
    assertThat(mappingA.runs().get(0).sourceOffset()).isEqualTo(0L);
    assertThat(mappingA.runs().get(0).targetOffset()).isEqualTo(0L);
    assertThat(mappingA.runs().get(0).length()).isEqualTo(100L);

    // Verify sourceB mapping
    RewriteFileGroup.FilePositionMapping mappingB = mappings.get("sourceB.parquet");
    assertThat(mappingB.targetFile()).isEqualTo("target1.parquet");
    assertThat(mappingB.runs()).hasSize(1);
    assertThat(mappingB.runs().get(0).sourceOffset()).isEqualTo(0L);
    assertThat(mappingB.runs().get(0).targetOffset()).isEqualTo(100L);
    assertThat(mappingB.runs().get(0).length()).isEqualTo(50L);

    // Verify sourceC mapping
    RewriteFileGroup.FilePositionMapping mappingC = mappings.get("sourceC.parquet");
    assertThat(mappingC.targetFile()).isEqualTo("target1.parquet");
    assertThat(mappingC.runs()).hasSize(1);
    assertThat(mappingC.runs().get(0).sourceOffset()).isEqualTo(0L);
    assertThat(mappingC.runs().get(0).targetOffset()).isEqualTo(150L);
    assertThat(mappingC.runs().get(0).length()).isEqualTo(75L);
  }

  @Test
  public void testSingleSourceFileToMultipleTargets() {
    // Single source file split into multiple targets - this should work
    // Each run stores its own target file path for multi-target support.

    // SourceA positions 0-99 -> target1
    for (int i = 0; i < 100; i++) {
      coordinator.recordMapping(
          mockTable, fileSetId, "sourceA.parquet", (long) i, "target1.parquet", (long) i);
    }

    // SourceA positions 100-199 -> target2
    for (int i = 100; i < 200; i++) {
      coordinator.recordMapping(
          mockTable, fileSetId, "sourceA.parquet", (long) i, "target2.parquet", (long) (i - 100));
    }

    Map<String, RewriteFileGroup.FilePositionMapping> mappings =
        coordinator.fetchMappings(mockTable, fileSetId);

    assertThat(mappings).hasSize(1);
    RewriteFileGroup.FilePositionMapping mapping = mappings.get("sourceA.parquet");
    assertThat(mapping).isNotNull();
    assertThat(mapping.sourceFile()).isEqualTo("sourceA.parquet");

    // Should have 2 runs (one per target file)
    assertThat(mapping.runs()).hasSize(2);

    // Verify first run goes to target1
    RewriteFileGroup.FilePositionMapping.Run run1 = mapping.runs().get(0);
    assertThat(run1.sourceOffset()).isEqualTo(0L);
    assertThat(run1.targetOffset()).isEqualTo(0L);
    assertThat(run1.length()).isEqualTo(100L);
    assertThat(run1.targetFile()).isEqualTo("target1.parquet");

    // Verify second run goes to target2
    RewriteFileGroup.FilePositionMapping.Run run2 = mapping.runs().get(1);
    assertThat(run2.sourceOffset()).isEqualTo(100L);
    assertThat(run2.targetOffset()).isEqualTo(0L);
    assertThat(run2.length()).isEqualTo(100L);
    assertThat(run2.targetFile()).isEqualTo("target2.parquet");

    // Verify multi-target helpers
    assertThat(mapping.isMultiTarget()).isTrue();
    assertThat(mapping.targetFiles()).containsExactlyInAnyOrder("target1.parquet", "target2.parquet");
    assertThat(mapping.targetFileForRun(run1)).isEqualTo("target1.parquet");
    assertThat(mapping.targetFileForRun(run2)).isEqualTo("target2.parquet");
  }

  @Test
  public void testEmptyMappings() {
    Map<String, RewriteFileGroup.FilePositionMapping> mappings =
        coordinator.fetchMappings(mockTable, fileSetId);

    assertThat(mappings).isEmpty();
  }

  @Test
  public void testClearRewrite() {
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 0L, "target1.parquet", 0L);
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 1L, "target1.parquet", 1L);

    Map<String, RewriteFileGroup.FilePositionMapping> mappings =
        coordinator.fetchMappings(mockTable, fileSetId);
    assertThat(mappings).hasSize(1);

    // Clear and verify empty
    coordinator.clearRewrite(mockTable, fileSetId);
    mappings = coordinator.fetchMappings(mockTable, fileSetId);
    assertThat(mappings).isEmpty();
  }

  @Test
  public void testMultipleFileSetIsolation() {
    String fileSetId1 = "fileset-1";
    String fileSetId2 = "fileset-2";

    // Record mappings for fileSetId1
    coordinator.recordMapping(mockTable, fileSetId1, "sourceA.parquet", 0L, "target1.parquet", 0L);

    // Record mappings for fileSetId2
    coordinator.recordMapping(mockTable, fileSetId2, "sourceB.parquet", 0L, "target2.parquet", 0L);

    Map<String, RewriteFileGroup.FilePositionMapping> mappings1 =
        coordinator.fetchMappings(mockTable, fileSetId1);
    Map<String, RewriteFileGroup.FilePositionMapping> mappings2 =
        coordinator.fetchMappings(mockTable, fileSetId2);

    assertThat(mappings1).hasSize(1);
    assertThat(mappings1.containsKey("sourceA.parquet")).isTrue();

    assertThat(mappings2).hasSize(1);
    assertThat(mappings2.containsKey("sourceB.parquet")).isTrue();

    // Clean up
    coordinator.clearRewrite(mockTable, fileSetId1);
    coordinator.clearRewrite(mockTable, fileSetId2);
  }

  @Test
  public void testConcurrentWrites() throws InterruptedException {
    int numThreads = 10;
    int recordsPerThread = 100;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(numThreads);
    AtomicInteger errorCount = new AtomicInteger(0);

    for (int t = 0; t < numThreads; t++) {
      final int threadId = t;
      final long basePosition = threadId * recordsPerThread;

      executor.submit(
          () -> {
            try {
              startLatch.await(); // Wait for all threads to be ready
              for (int i = 0; i < recordsPerThread; i++) {
                coordinator.recordMapping(
                    mockTable,
                    fileSetId,
                    "source.parquet",
                    basePosition + i,
                    "target.parquet",
                    basePosition + i);
              }
            } catch (Exception e) {
              errorCount.incrementAndGet();
              e.printStackTrace();
            } finally {
              completionLatch.countDown();
            }
          });
    }

    // Start all threads simultaneously
    startLatch.countDown();

    // Wait for completion
    boolean completed = completionLatch.await(30, TimeUnit.SECONDS);
    assertThat(completed).isTrue();
    assertThat(errorCount.get()).isEqualTo(0);

    Map<String, RewriteFileGroup.FilePositionMapping> mappings =
        coordinator.fetchMappings(mockTable, fileSetId);

    assertThat(mappings).hasSize(1);
    RewriteFileGroup.FilePositionMapping mapping = mappings.get("source.parquet");
    assertThat(mapping.runs()).hasSize(1);
    assertThat(mapping.runs().get(0).length()).isEqualTo(numThreads * recordsPerThread);

    executor.shutdown();
  }

  @Test
  public void testOutOfOrderRecording() {
    // Record positions out of order - coordinator should sort them
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 3L, "target1.parquet", 3L);
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 1L, "target1.parquet", 1L);
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 0L, "target1.parquet", 0L);
    coordinator.recordMapping(mockTable, fileSetId, "sourceA.parquet", 2L, "target1.parquet", 2L);

    Map<String, RewriteFileGroup.FilePositionMapping> mappings =
        coordinator.fetchMappings(mockTable, fileSetId);

    assertThat(mappings).hasSize(1);
    RewriteFileGroup.FilePositionMapping mapping = mappings.get("sourceA.parquet");
    assertThat(mapping.runs()).hasSize(1);

    RewriteFileGroup.FilePositionMapping.Run run = mapping.runs().get(0);
    assertThat(run.sourceOffset()).isEqualTo(0L);
    assertThat(run.targetOffset()).isEqualTo(0L);
    assertThat(run.length()).isEqualTo(4L);
  }
}
