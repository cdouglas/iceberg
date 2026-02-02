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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

/** Tests for {@link CompactionMapChain}. */
public class TestCompactionMapChain {

  @Test
  public void testSingleMapChain() {
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    CompactionMap m1 = m1Builder.build();

    CompactionMapChain chain = CompactionMapChain.build(ImmutableList.of(m1));

    assertThat(chain.size()).isEqualTo(1);
    assertThat(chain.firstSourceSnapshotId()).isEqualTo(1L);
    assertThat(chain.lastTargetSnapshotId()).isEqualTo(2L);
    assertThat(chain.containsSource("F1")).isTrue();
    assertThat(chain.containsSource("F2")).isFalse();

    FileMapping mapping = chain.mappingForFile("F1");
    assertThat(mapping).isNotNull();
    assertThat(mapping.targetFile()).isEqualTo("F2");

    Run run = mapping.runForPosition(50);
    assertThat(run).isNotNull();
    assertThat(run.mapPosition(50)).isEqualTo(50);
  }

  @Test
  public void testTwoMapChain() {
    // M1: F1 -> F2 (rows 0-99 -> 0-99)
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    CompactionMap m1 = m1Builder.build();

    // M2: F2 -> F3 (rows 0-99 -> 100-199)
    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 100, 100);
    CompactionMap m2 = m2Builder.build();

    CompactionMapChain chain = CompactionMapChain.build(ImmutableList.of(m1, m2));

    assertThat(chain.size()).isEqualTo(2);
    assertThat(chain.firstSourceSnapshotId()).isEqualTo(1L);
    assertThat(chain.lastTargetSnapshotId()).isEqualTo(3L);
    assertThat(chain.containsSource("F1")).isTrue();
    assertThat(chain.containsSource("F2")).isTrue();
    assertThat(chain.containsSource("F3")).isFalse();

    // F1 should be composed through to F3
    FileMapping f1Mapping = chain.mappingForFile("F1");
    assertThat(f1Mapping).isNotNull();
    assertThat(f1Mapping.targetFile()).isEqualTo("F3");

    Run run = f1Mapping.runForPosition(50);
    assertThat(run).isNotNull();
    assertThat(run.mapPosition(50)).isEqualTo(150); // 0+50 -> 100+50

    // F2 should map directly to F3 (no composition needed)
    FileMapping f2Mapping = chain.mappingForFile("F2");
    assertThat(f2Mapping).isNotNull();
    assertThat(f2Mapping.targetFile()).isEqualTo("F3");
  }

  @Test
  public void testThreeMapChain() {
    // F1 -> F2 -> F3 -> F4
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    CompactionMap m1 = m1Builder.build();

    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 50, 100);
    CompactionMap m2 = m2Builder.build();

    CompactionMapBuilder m3Builder = new CompactionMapBuilder(3L, 4L);
    m3Builder.addFileMapping("F3", "F4").addRun(0, 200, 200);
    CompactionMap m3 = m3Builder.build();

    CompactionMapChain chain = CompactionMapChain.build(ImmutableList.of(m1, m2, m3));

    assertThat(chain.size()).isEqualTo(3);
    assertThat(chain.firstSourceSnapshotId()).isEqualTo(1L);
    assertThat(chain.lastTargetSnapshotId()).isEqualTo(4L);

    // F1 should compose through entire chain
    FileMapping f1Mapping = chain.mappingForFile("F1");
    assertThat(f1Mapping).isNotNull();
    assertThat(f1Mapping.targetFile()).isEqualTo("F4");

    // Position 25: F1[25] -> F2[25] -> F3[75] -> F4[275]
    Run run = f1Mapping.runForPosition(25);
    assertThat(run).isNotNull();
    assertThat(run.mapPosition(25)).isEqualTo(275);
  }

  @Test
  public void testChainWithParallelCompactions() {
    // M1 compacts both F1->F2 and G1->G2
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    m1Builder.addFileMapping("G1", "G2").addRun(0, 0, 50);
    CompactionMap m1 = m1Builder.build();

    // M2 only compacts F2->F3, G2 is unchanged
    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 0, 100);
    CompactionMap m2 = m2Builder.build();

    CompactionMapChain chain = CompactionMapChain.build(ImmutableList.of(m1, m2));

    // F1 should compose to F3
    FileMapping f1Mapping = chain.mappingForFile("F1");
    assertThat(f1Mapping).isNotNull();
    assertThat(f1Mapping.targetFile()).isEqualTo("F3");

    // G1 should map to G2 (not composed further)
    FileMapping g1Mapping = chain.mappingForFile("G1");
    assertThat(g1Mapping).isNotNull();
    assertThat(g1Mapping.targetFile()).isEqualTo("G2");
  }

  @Test
  public void testMappingCaching() {
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    CompactionMap m1 = m1Builder.build();

    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 0, 100);
    CompactionMap m2 = m2Builder.build();

    CompactionMapChain chain = CompactionMapChain.build(ImmutableList.of(m1, m2));

    // First call computes the mapping
    FileMapping mapping1 = chain.mappingForFile("F1");
    // Second call should return cached mapping
    FileMapping mapping2 = chain.mappingForFile("F1");

    // Should be the same object (cached)
    assertThat(mapping1).isSameAs(mapping2);
  }

  @Test
  public void testSourceFiles() {
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    m1Builder.addFileMapping("G1", "G2").addRun(0, 0, 50);
    CompactionMap m1 = m1Builder.build();

    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 0, 100);
    m2Builder.addFileMapping("H1", "H2").addRun(0, 0, 25);
    CompactionMap m2 = m2Builder.build();

    CompactionMapChain chain = CompactionMapChain.build(ImmutableList.of(m1, m2));

    assertThat(chain.sourceFiles()).containsExactlyInAnyOrder("F1", "G1", "F2", "H1");
  }

  @Test
  public void testMappingForNonExistentFile() {
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    CompactionMap m1 = m1Builder.build();

    CompactionMapChain chain = CompactionMapChain.build(ImmutableList.of(m1));

    FileMapping mapping = chain.mappingForFile("nonexistent");
    assertThat(mapping).isNull();
  }

  @Test
  public void testEmptyChainThrows() {
    assertThatThrownBy(() -> CompactionMapChain.build(ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testNullChainThrows() {
    assertThatThrownBy(() -> CompactionMapChain.build(null))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
