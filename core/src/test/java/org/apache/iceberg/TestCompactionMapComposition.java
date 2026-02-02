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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.CompactionMap.FileMapping;
import org.apache.iceberg.CompactionMap.Run;
import org.junit.jupiter.api.Test;

/** Tests for {@link CompactionMaps#compose(CompactionMap, CompactionMap)}. */
public class TestCompactionMapComposition {

  @Test
  public void testSimpleComposition() {
    // M1: F1 -> F2 (rows 0-99 -> 0-99)
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    CompactionMap m1 = m1Builder.build();

    // M2: F2 -> F3 (rows 0-99 -> 100-199)
    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 100, 100);
    CompactionMap m2 = m2Builder.build();

    // Composed: F1 -> F3 (rows 0-99 -> 100-199)
    CompactionMap composed = CompactionMaps.compose(m1, m2);

    assertThat(composed.sourceSnapshotId()).isEqualTo(1L);
    assertThat(composed.targetSnapshotId()).isEqualTo(3L);
    assertThat(composed.fileMappings()).hasSize(1);

    FileMapping mapping = composed.mappingForFile("F1");
    assertThat(mapping).isNotNull();
    assertThat(mapping.targetFile()).isEqualTo("F3");

    // Verify position mapping: F1[50] -> F3[150]
    Run run = mapping.runForPosition(50);
    assertThat(run).isNotNull();
    assertThat(run.mapPosition(50)).isEqualTo(150);
  }

  @Test
  public void testCompositionWithMultipleRuns() {
    // M1: F1 -> F2
    //   Run1: rows 0-49 -> 0-49
    //   Run2: rows 100-149 -> 50-99 (gap in source)
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 50).addRun(100, 50, 50);
    CompactionMap m1 = m1Builder.build();

    // M2: F2 -> F3
    //   Run1: rows 0-99 -> 200-299
    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 200, 100);
    CompactionMap m2 = m2Builder.build();

    // Composed: F1 -> F3
    //   Run1: rows 0-49 -> 200-249 (from M1.Run1 overlapping with M2.Run1)
    //   Run2: rows 100-149 -> 250-299 (from M1.Run2 overlapping with M2.Run1)
    CompactionMap composed = CompactionMaps.compose(m1, m2);

    FileMapping mapping = composed.mappingForFile("F1");
    assertThat(mapping).isNotNull();

    // Position 25 in F1 -> position 0+25=25 in F2 -> position 200+25=225 in F3
    Run run1 = mapping.runForPosition(25);
    assertThat(run1).isNotNull();
    assertThat(run1.mapPosition(25)).isEqualTo(225);

    // Position 125 in F1 -> position 50+(125-100)=75 in F2 -> position 200+75=275 in F3
    Run run2 = mapping.runForPosition(125);
    assertThat(run2).isNotNull();
    assertThat(run2.mapPosition(125)).isEqualTo(275);
  }

  @Test
  public void testCompositionWithPartialOverlap() {
    // M1: F1 -> F2 (rows 0-99 -> 50-149)
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 50, 100);
    CompactionMap m1 = m1Builder.build();

    // M2: F2 -> F3 (rows 0-99 -> 0-99) - only partial overlap with M1's target range
    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 0, 100);
    CompactionMap m2 = m2Builder.build();

    // Composed: F1 -> F3
    // M1 maps F1[0-99] to F2[50-149]
    // M2 maps F2[0-99] to F3[0-99]
    // Overlap: F2[50-99] (50 positions)
    // This corresponds to F1[0-49] -> F3[50-99]
    CompactionMap composed = CompactionMaps.compose(m1, m2);

    FileMapping mapping = composed.mappingForFile("F1");
    assertThat(mapping).isNotNull();

    // Position 25 in F1 -> position 50+25=75 in F2 -> position 0+75=75 in F3
    Run run = mapping.runForPosition(25);
    assertThat(run).isNotNull();
    assertThat(run.mapPosition(25)).isEqualTo(75);

    // Position 75 should NOT be mappable (F2[125] is outside M2's range)
    Run run2 = mapping.runForPosition(75);
    assertThat(run2).isNull();
  }

  @Test
  public void testCompositionWithNoOverlap() {
    // M1: F1 -> F2 (rows 0-99 -> 0-99)
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    CompactionMap m1 = m1Builder.build();

    // M2: F2 -> F3 (rows 200-299 -> 0-99) - no overlap with M1's target range
    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(200, 0, 100);
    CompactionMap m2 = m2Builder.build();

    // Composed: F1 -> F2 (unchanged, since F2[0-99] is not in M2)
    CompactionMap composed = CompactionMaps.compose(m1, m2);

    FileMapping mapping = composed.mappingForFile("F1");
    assertThat(mapping).isNotNull();
    assertThat(mapping.targetFile()).isEqualTo("F2");

    Run run = mapping.runForPosition(50);
    assertThat(run).isNotNull();
    assertThat(run.mapPosition(50)).isEqualTo(50);
  }

  @Test
  public void testCompositionWithMultiTargetMaps() {
    // M1: F1 -> F2a and F2b (multi-target)
    //   Rows 0-49 -> F2a[0-49]
    //   Rows 50-99 -> F2b[0-49]
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2a").addRun(0, 0, 50, "F2a").addRun(50, 0, 50, "F2b");
    CompactionMap m1 = m1Builder.build();

    // M2: F2a -> F3 (rows 0-49 -> 100-149)
    // M2: F2b -> F3 (rows 0-49 -> 150-199)
    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2a", "F3").addRun(0, 100, 50);
    m2Builder.addFileMapping("F2b", "F3").addRun(0, 150, 50);
    CompactionMap m2 = m2Builder.build();

    // Composed: F1 -> F3
    //   Rows 0-49 (from F2a) -> 100-149
    //   Rows 50-99 (from F2b) -> 150-199
    CompactionMap composed = CompactionMaps.compose(m1, m2);

    FileMapping mapping = composed.mappingForFile("F1");
    assertThat(mapping).isNotNull();

    // Position 25 in F1 -> F2a[25] -> F3[125]
    Run run1 = mapping.runForPosition(25);
    assertThat(run1).isNotNull();
    assertThat(run1.mapPosition(25)).isEqualTo(125);

    // Position 75 in F1 -> F2b[25] -> F3[175]
    Run run2 = mapping.runForPosition(75);
    assertThat(run2).isNotNull();
    assertThat(run2.mapPosition(75)).isEqualTo(175);
  }

  @Test
  public void testCompositionWithIntermediateFileNotInSecondMap() {
    // M1: F1 -> F2 (rows 0-99 -> 0-99)
    // M1: G1 -> G2 (rows 0-49 -> 0-49)
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    m1Builder.addFileMapping("G1", "G2").addRun(0, 0, 50);
    CompactionMap m1 = m1Builder.build();

    // M2: F2 -> F3 (rows 0-99 -> 0-99), but G2 is not in M2
    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 0, 100);
    CompactionMap m2 = m2Builder.build();

    // Composed:
    //   F1 -> F3 (through composition)
    //   G1 -> G2 (unchanged, since G2 is not in M2)
    CompactionMap composed = CompactionMaps.compose(m1, m2);

    assertThat(composed.fileMappings()).hasSize(2);

    // F1 should be composed through to F3
    FileMapping f1Mapping = composed.mappingForFile("F1");
    assertThat(f1Mapping).isNotNull();
    assertThat(f1Mapping.targetFile()).isEqualTo("F3");

    // G1 should map to G2 (unchanged)
    FileMapping g1Mapping = composed.mappingForFile("G1");
    assertThat(g1Mapping).isNotNull();
    assertThat(g1Mapping.targetFile()).isEqualTo("G2");
  }

  @Test
  public void testCompositionSnapshotIdMismatchThrows() {
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    CompactionMap m1 = m1Builder.build();

    // M2's source snapshot doesn't match M1's target
    CompactionMapBuilder m2Builder = new CompactionMapBuilder(5L, 6L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 0, 100);
    CompactionMap m2 = m2Builder.build();

    assertThatThrownBy(() -> CompactionMaps.compose(m1, m2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("m1.targetSnapshotId")
        .hasMessageContaining("m2.sourceSnapshotId");
  }

  @Test
  public void testThreeMapChainComposition() {
    // Chain: F1 -> F2 -> F3 -> F4
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    CompactionMap m1 = m1Builder.build();

    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 50, 100); // Offset by 50
    CompactionMap m2 = m2Builder.build();

    CompactionMapBuilder m3Builder = new CompactionMapBuilder(3L, 4L);
    m3Builder.addFileMapping("F3", "F4").addRun(0, 200, 200); // Offset by 200
    CompactionMap m3 = m3Builder.build();

    // Compose m1 and m2 first
    CompactionMap m12 = CompactionMaps.compose(m1, m2);

    // Then compose with m3
    CompactionMap m123 = CompactionMaps.compose(m12, m3);

    assertThat(m123.sourceSnapshotId()).isEqualTo(1L);
    assertThat(m123.targetSnapshotId()).isEqualTo(4L);

    FileMapping mapping = m123.mappingForFile("F1");
    assertThat(mapping).isNotNull();
    assertThat(mapping.targetFile()).isEqualTo("F4");

    // Position 25 in F1 -> F2[25] -> F3[75] -> F4[275]
    Run run = mapping.runForPosition(25);
    assertThat(run).isNotNull();
    assertThat(run.mapPosition(25)).isEqualTo(275);
  }
}
