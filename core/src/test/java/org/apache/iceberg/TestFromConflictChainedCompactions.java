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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.exceptions.ChainedCompactionMapsException;
import org.apache.iceberg.exceptions.CompactionConflictException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests verifying that {@link ChainedCompactionMapsException} extends {@link
 * CompactionConflictException} and that {@link PositionDeleteRemapper#fromConflict} handles both
 * single and chained compaction conflicts correctly.
 */
public class TestFromConflictChainedCompactions {

  @TempDir public File temp;

  private InMemoryCatalog catalog;
  private FileIO fileIO;

  @BeforeEach
  public void setup() {
    catalog = new InMemoryCatalog();
    catalog.initialize("test", Collections.emptyMap());
    catalog.createNamespace(Namespace.of("db"));

    // Create a table just for its FileIO
    Table table =
        catalog.createTable(
            TableIdentifier.of("db", "io_table"),
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));
    fileIO = table.io();
  }

  @Test
  public void testChainedExceptionIsCompactionConflictException() {
    ChainedCompactionMapsException chained =
        new ChainedCompactionMapsException(Set.of("F1"), List.of(1L, 2L, 3L), List.of());

    // ChainedCompactionMapsException must be catchable as CompactionConflictException
    assertThat(chained).isInstanceOf(CompactionConflictException.class);
    assertThat(chained).isInstanceOf(ValidationException.class);

    // compactedFiles() from CompactionConflictException should return the chained files
    CompactionConflictException asConflict = chained;
    assertThat(asConflict.compactedFiles()).containsExactly("F1");
  }

  @Test
  public void testCatchCompactionConflictCatchesChained() {
    ChainedCompactionMapsException chained =
        new ChainedCompactionMapsException(Set.of("F1"), List.of(1L, 2L), List.of());

    // Simulate the integrator catch pattern
    boolean caught = false;
    try {
      throw chained;
    } catch (CompactionConflictException e) {
      // Integrators catching CompactionConflictException should also catch chained
      caught = true;
      assertThat(e).isInstanceOf(ChainedCompactionMapsException.class);
    }

    assertThat(caught).as("ChainedCompactionMapsException should be caught by CompactionConflictException handler").isTrue();
  }

  @Test
  public void testFromConflictWithChainedCompaction() throws IOException {
    // Build chain: F1 -> F2 (map M1), F2 -> F3 (map M2)
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    CompactionMap m1 = m1Builder.build();

    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 200, 100);
    CompactionMap m2 = m2Builder.build();

    // Create chained exception (maps are pre-loaded, not from files)
    ChainedCompactionMapsException chainedException =
        new ChainedCompactionMapsException(
            Set.of("F1"), List.of(1L, 2L, 3L), List.of(m1, m2));

    // fromConflict should handle ChainedCompactionMapsException transparently
    Map<String, PositionDeleteRemapper> remappers =
        PositionDeleteRemapper.fromConflict(chainedException, fileIO);

    assertThat(remappers).containsKey("F1");

    // Verify the remapper composes the chain correctly: F1[50] -> F2[50] -> F3[250]
    PositionDeleteRemapper remapper = remappers.get("F1");
    PositionDelete<?> delete = PositionDelete.create();
    delete.set("F1", 50L, null);

    PositionDelete<?> remapped = remapper.remapDeleteOrNull(delete);
    assertThat(remapped).isNotNull();
    assertThat(remapped.path().toString()).isEqualTo("F3");
    assertThat(remapped.pos()).isEqualTo(250L);
  }

  @Test
  public void testFromConflictWithChainedCompactionMultipleFiles() throws IOException {
    // Chain: F1 -> F2, G1 -> G2 (map M1), F2 -> F3 (map M2)
    CompactionMapBuilder m1Builder = new CompactionMapBuilder(1L, 2L);
    m1Builder.addFileMapping("F1", "F2").addRun(0, 0, 100);
    m1Builder.addFileMapping("G1", "G2").addRun(0, 0, 50);
    CompactionMap m1 = m1Builder.build();

    CompactionMapBuilder m2Builder = new CompactionMapBuilder(2L, 3L);
    m2Builder.addFileMapping("F2", "F3").addRun(0, 0, 100);
    CompactionMap m2 = m2Builder.build();

    ChainedCompactionMapsException chainedException =
        new ChainedCompactionMapsException(
            Set.of("F1", "G1"), List.of(1L, 2L, 3L), List.of(m1, m2));

    Map<String, PositionDeleteRemapper> remappers =
        PositionDeleteRemapper.fromConflict(chainedException, fileIO);

    // Both chained files should have remappers
    assertThat(remappers).containsKeys("F1", "G1");

    // F1 should remap through F2 to F3
    PositionDelete<?> f1Delete = PositionDelete.create();
    f1Delete.set("F1", 25L, null);
    PositionDelete<?> f1Remapped = remappers.get("F1").remapDeleteOrNull(f1Delete);
    assertThat(f1Remapped).isNotNull();
    assertThat(f1Remapped.path().toString()).isEqualTo("F3");
    assertThat(f1Remapped.pos()).isEqualTo(25L);

    // G1 should remap to G2 (only in M1, not chained further)
    PositionDelete<?> g1Delete = PositionDelete.create();
    g1Delete.set("G1", 10L, null);
    PositionDelete<?> g1Remapped = remappers.get("G1").remapDeleteOrNull(g1Delete);
    assertThat(g1Remapped).isNotNull();
    assertThat(g1Remapped.path().toString()).isEqualTo("G2");
    assertThat(g1Remapped.pos()).isEqualTo(10L);
  }

  @Test
  public void testFromConflictWithSingleCompaction() throws IOException {
    // Write a single compaction map to a real file
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping("F1", "F2").addRun(0, 100, 50);
    CompactionMap map = builder.build();

    // Create a table to get a metadata location for writing
    TableIdentifier tableIdent = TableIdentifier.of("db", "single_map_table");
    Table table =
        catalog.createTable(
            tableIdent,
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));

    org.apache.iceberg.io.OutputFile mapFile =
        CompactionMaps.newCompactionMapFile(table, 2L);
    CompactionMaps.write(map, mapFile);

    // Create single-map CompactionConflictException
    CompactionConflictException singleConflict =
        new CompactionConflictException(
            "Test conflict",
            Set.of("F1"),
            java.util.Map.of("F1", mapFile.location()));

    // fromConflict should handle single-map conflicts
    Map<String, PositionDeleteRemapper> remappers =
        PositionDeleteRemapper.fromConflict(singleConflict, table.io());

    assertThat(remappers).containsKey("F1");

    PositionDelete<?> delete = PositionDelete.create();
    delete.set("F1", 25L, null);

    PositionDelete<?> remapped = remappers.get("F1").remapDeleteOrNull(delete);
    assertThat(remapped).isNotNull();
    assertThat(remapped.path().toString()).isEqualTo("F2");
    assertThat(remapped.pos()).isEqualTo(125L);
  }
}
