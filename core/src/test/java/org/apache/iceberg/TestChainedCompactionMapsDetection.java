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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.exceptions.ChainedCompactionMapsException;
import org.junit.jupiter.api.Test;

/**
 * Tests for chained compaction map detection.
 *
 * <p>Note: These are unit tests for the composition and chain classes. Integration tests with
 * actual table operations would require the full Iceberg test infrastructure.
 */
public class TestChainedCompactionMapsDetection {

  @Test
  public void testChainedCompactionMapsExceptionMessage() {
    // Create exception with test data
    ChainedCompactionMapsException exception =
        new ChainedCompactionMapsException(
            java.util.Set.of("F1", "F2"), java.util.List.of(1L, 2L, 3L), java.util.List.of());

    assertThat(exception.getMessage())
        .contains("2 file(s) require chained compaction map composition")
        .contains("[1, 2, 3]");

    assertThat(exception.chainedFiles()).containsExactlyInAnyOrder("F1", "F2");
    assertThat(exception.chainSnapshotIds()).containsExactly(1L, 2L, 3L);
  }

  @Test
  public void testChainedCompactionMapsExceptionWithMaps() {
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    CompactionMap m1 = m1Builder.build();

    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 0, 100);
    CompactionMap m2 = m2Builder.build();

    ChainedCompactionMapsException exception =
        new ChainedCompactionMapsException(
            java.util.Set.of("F1"), java.util.List.of(1L, 2L, 3L), java.util.List.of(m1, m2));

    assertThat(exception.compactionMaps()).hasSize(2);
    assertThat(exception.compactionMaps().get(0).sourceSnapshotId()).isEqualTo(1L);
    assertThat(exception.compactionMaps().get(1).sourceSnapshotId()).isEqualTo(2L);
  }

  @Test
  public void testPositionDeleteRemapperWithChain() {
    // Create a chain: F1 -> F2 -> F3
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    CompactionMap m1 = m1Builder.build();

    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 100, 100); // Offset by 100 in target
    CompactionMap m2 = m2Builder.build();

    CompactionMapChain chain = CompactionMapChain.build(java.util.List.of(m1, m2));

    // Create remapper with chain
    PositionDeleteRemapper remapper = new PositionDeleteRemapper(chain);

    // F1 should be recognized as compacted
    assertThat(remapper.isCompacted("F1")).isTrue();
    assertThat(remapper.isCompacted("F2")).isTrue();
    assertThat(remapper.isCompacted("F3")).isFalse();

    // Create a position delete for F1
    org.apache.iceberg.deletes.PositionDelete<?> delete =
        org.apache.iceberg.deletes.PositionDelete.create();
    delete.set("F1", 50L, null);

    // Remap should go through the chain: F1[50] -> F2[50] -> F3[150]
    org.apache.iceberg.deletes.PositionDelete<?> remapped = remapper.remapDeleteOrNull(delete);

    assertThat(remapped).isNotNull();
    assertThat(remapped.path().toString()).isEqualTo("F3");
    assertThat(remapped.pos()).isEqualTo(150L);
  }

  @Test
  public void testPositionDeleteRemapperWithChainUnmappedPosition() {
    // Create a chain with partial coverage
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 50); // Only rows 0-49
    CompactionMap m1 = m1Builder.build();

    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 0, 50);
    CompactionMap m2 = m2Builder.build();

    CompactionMapChain chain = CompactionMapChain.build(java.util.List.of(m1, m2));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(chain);

    // Position 75 in F1 is not covered by the first map
    org.apache.iceberg.deletes.PositionDelete<?> delete =
        org.apache.iceberg.deletes.PositionDelete.create();
    delete.set("F1", 75L, null);

    // Should return null since position is not in the mapping
    org.apache.iceberg.deletes.PositionDelete<?> remapped = remapper.remapDeleteOrNull(delete);
    assertThat(remapped).isNull();
  }

  @Test
  public void testPositionDeleteRemapperWithChainUncompactedFile() {
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    CompactionMap m1 = m1Builder.build();

    CompactionMapChain chain = CompactionMapChain.build(java.util.List.of(m1));

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(chain);

    // Position delete for file not in the chain
    org.apache.iceberg.deletes.PositionDelete<?> delete =
        org.apache.iceberg.deletes.PositionDelete.create();
    delete.set("uncompacted_file.parquet", 50L, null);

    // Should return original delete unchanged
    org.apache.iceberg.deletes.PositionDelete<?> remapped = remapper.remapDeleteOrNull(delete);
    assertThat(remapped).isSameAs(delete);
  }
}
