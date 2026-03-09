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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.junit.jupiter.api.Test;

/**
 * Tests verifying that PositionDeleteRemapper is NOT Java-serializable.
 *
 * <p>This test documents the serialization gap identified in review: any Spark closure that
 * captures a PositionDeleteRemapper as a non-transient field will fail with
 * NotSerializableException when shipped to executors. The correct pattern is to store CompactionMap
 * (Avro-serializable) and lazily initialize the remapper on the executor side.
 *
 * @see PositionDeleteRemapper
 */
public class TestRemapFunctionSerializability {

  @Test
  public void testPositionDeleteRemapperIsNotSerializable() {
    // PositionDeleteRemapper does not implement Serializable.
    // This is by design — it holds mutable strategy state.
    assertThat(Serializable.class.isAssignableFrom(PositionDeleteRemapper.class)).isFalse();
  }

  @Test
  public void testCapturingRemapperInSerializableClosureFails() {
    // Build a minimal compaction map
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping("/source.parquet", "/target.parquet").addRun(0, 0, 100);
    CompactionMap map = builder.build();

    PositionDeleteRemapper remapper = new PositionDeleteRemapper(map);

    // Simulate what RemapFunctionWithRemapper does: capture remapper in a Serializable wrapper
    SerializableWrapperWithRemapper wrapper = new SerializableWrapperWithRemapper(remapper);

    assertThatThrownBy(() -> roundTripSerialize(wrapper))
        .isInstanceOf(java.io.NotSerializableException.class)
        .hasMessageContaining("PositionDeleteRemapper");
  }

  @Test
  public void testCapturingCompactionMapFieldsSurvivesSerialization() throws Exception {
    // Demonstrate the safe pattern: store snapshot IDs and file mapping data,
    // reconstruct the remapper lazily on the other side.
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping("/source.parquet", "/target.parquet").addRun(0, 0, 100);
    CompactionMap map = builder.build();

    // The safe wrapper stores only primitive/Serializable data
    SafeSerializableWrapper wrapper = new SafeSerializableWrapper(map);

    SafeSerializableWrapper deserialized = roundTripSerialize(wrapper);
    assertThat(deserialized).isNotNull();
    assertThat(deserialized.getSourceSnapshotId()).isEqualTo(1L);
    assertThat(deserialized.getTargetSnapshotId()).isEqualTo(2L);
  }

  @SuppressWarnings("unchecked")
  private static <T extends Serializable> T roundTripSerialize(T obj) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(obj);
    }
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      return (T) ois.readObject();
    }
  }

  /**
   * Simulates RemapFunctionWithRemapper's problematic pattern: captures a non-serializable
   * PositionDeleteRemapper in a Serializable closure.
   */
  private static class SerializableWrapperWithRemapper implements Serializable {
    @SuppressWarnings("unused")
    private final PositionDeleteRemapper remapper;

    SerializableWrapperWithRemapper(PositionDeleteRemapper remapper) {
      this.remapper = remapper;
    }
  }

  /**
   * Verifies that CompactionMaps.toBytes/fromBytes round-trip preserves map content, and that a
   * closure storing byte[] (the actual fix pattern) survives Java serialization.
   */
  @Test
  public void testAvroBytesRoundTripAndSerializableClosure() throws Exception {
    CompactionMapBuilder builder = new CompactionMapBuilder(1L, 2L);
    builder.addFileMapping("/source.parquet", "/target.parquet").addRun(0, 0, 100);
    CompactionMap original = builder.build();

    // Verify Avro byte round-trip preserves data
    byte[] bytes = CompactionMaps.toBytes(original);
    assertThat(bytes).isNotEmpty();
    CompactionMap restored = CompactionMaps.fromBytes(bytes);
    assertThat(restored.sourceSnapshotId()).isEqualTo(1L);
    assertThat(restored.targetSnapshotId()).isEqualTo(2L);
    assertThat(restored.fileMappings()).hasSize(1);
    assertThat(restored.fileMappings().get(0).sourceFile()).isEqualTo("/source.parquet");

    // Verify a closure storing byte[] survives Java serialization (the actual fix pattern)
    AvroBytesWrapper wrapper = new AvroBytesWrapper(bytes);
    AvroBytesWrapper deserialized = roundTripSerialize(wrapper);
    CompactionMap fromDeserialized = CompactionMaps.fromBytes(deserialized.getMapBytes());
    assertThat(fromDeserialized.sourceSnapshotId()).isEqualTo(1L);
    assertThat(fromDeserialized.fileMappings()).hasSize(1);
  }

  /**
   * Demonstrates the safe pattern: store serializable metadata, reconstruct remapper lazily. This
   * mirrors how RemapFunction correctly handles the problem.
   */
  private static class SafeSerializableWrapper implements Serializable {
    private final long sourceSnapshotId;
    private final long targetSnapshotId;

    SafeSerializableWrapper(CompactionMap map) {
      this.sourceSnapshotId = map.sourceSnapshotId();
      this.targetSnapshotId = map.targetSnapshotId();
    }

    long getSourceSnapshotId() {
      return sourceSnapshotId;
    }

    long getTargetSnapshotId() {
      return targetSnapshotId;
    }
  }

  /** Mirrors the actual fix: store compaction map as Avro bytes in a Serializable closure. */
  private static class AvroBytesWrapper implements Serializable {
    private final byte[] mapBytes;

    AvroBytesWrapper(byte[] mapBytes) {
      this.mapBytes = mapBytes;
    }

    byte[] getMapBytes() {
      return mapBytes;
    }
  }
}
