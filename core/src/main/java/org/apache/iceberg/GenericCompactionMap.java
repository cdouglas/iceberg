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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData.SchemaConstructable;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.SupportsIndexProjection;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/**
 * Generic implementation of {@link CompactionMap} that supports Avro serialization.
 *
 * <p>This follows the same pattern as {@link GenericManifestFile}.
 */
public class GenericCompactionMap extends SupportsIndexProjection
    implements CompactionMap, StructLike, IndexedRecord, SchemaConstructable {

  private static final Schema AVRO_SCHEMA =
      AvroSchemaUtil.convert(CompactionMap.schema(), "compaction_map");

  private transient Schema avroSchema; // not final for Java serialization

  // Data fields
  private long sourceSnapshotId;
  private long targetSnapshotId;
  private FileMapping[] fileMappings;

  // Index for fast lookup
  private transient Map<String, FileMapping> fileMappingIndex;

  /** Used by Avro reflection to instantiate this class when reading compaction map files. */
  public GenericCompactionMap(Schema avroSchema) {
    super(CompactionMap.schema().asStruct(), AvroSchemaUtil.convert(avroSchema).asStructType());
    this.avroSchema = avroSchema;
  }

  /** Used by Avro reflection to instantiate this class when reading compaction map files. */
  GenericCompactionMap(Types.StructType projectedSchema) {
    super(CompactionMap.schema().asStruct(), projectedSchema);
    this.avroSchema = AVRO_SCHEMA;
  }

  /** Constructor for programmatic creation. */
  public GenericCompactionMap(
      long sourceSnapshotId, long targetSnapshotId, List<FileMapping> fileMappings) {
    super(CompactionMap.schema().columns().size());
    this.avroSchema = AVRO_SCHEMA;
    this.sourceSnapshotId = sourceSnapshotId;
    this.targetSnapshotId = targetSnapshotId;
    this.fileMappings = fileMappings.toArray(new FileMapping[0]);
    this.fileMappingIndex = null; // Built lazily
  }

  /** Copy constructor. */
  private GenericCompactionMap(GenericCompactionMap toCopy) {
    super(toCopy);
    this.avroSchema = toCopy.avroSchema;
    this.sourceSnapshotId = toCopy.sourceSnapshotId;
    this.targetSnapshotId = toCopy.targetSnapshotId;

    if (toCopy.fileMappings != null) {
      this.fileMappings =
          Stream.of(toCopy.fileMappings).map(FileMapping::copy).toArray(FileMapping[]::new);
    } else {
      this.fileMappings = null;
    }

    this.fileMappingIndex = null; // Rebuilt on demand
  }

  /** Constructor for Java serialization. */
  GenericCompactionMap() {
    super(CompactionMap.schema().columns().size());
  }

  @Override
  public long sourceSnapshotId() {
    return sourceSnapshotId;
  }

  @Override
  public long targetSnapshotId() {
    return targetSnapshotId;
  }

  @Override
  public List<FileMapping> fileMappings() {
    return fileMappings != null ? ImmutableList.copyOf(fileMappings) : ImmutableList.of();
  }

  @Override
  public FileMapping mappingForFile(String sourceFilePath) {
    if (fileMappingIndex == null) {
      buildIndex();
    }
    return fileMappingIndex.get(sourceFilePath);
  }

  private void buildIndex() {
    fileMappingIndex = new HashMap<>();
    if (fileMappings != null) {
      for (FileMapping mapping : fileMappings) {
        fileMappingIndex.put(mapping.sourceFile(), mapping);
      }
    }
  }

  @Override
  public CompactionMap copy() {
    return new GenericCompactionMap(this);
  }

  // StructLike implementation
  @Override
  public Object get(int pos) {
    return internalGet(pos, Object.class);
  }

  @Override
  protected <T> T internalGet(int pos, Class<T> javaClass) {
    return javaClass.cast(getByPos(pos));
  }

  private Object getByPos(int pos) {
    switch (pos) {
      case 0:
        return sourceSnapshotId;
      case 1:
        return targetSnapshotId;
      case 2:
        return fileMappings != null ? Arrays.asList(fileMappings) : Lists.newArrayList();
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  @Override
  protected <T> void internalSet(int pos, T value) {
    switch (pos) {
      case 0:
        this.sourceSnapshotId = (Long) value;
        return;
      case 1:
        this.targetSnapshotId = (Long) value;
        return;
      case 2:
        @SuppressWarnings("unchecked")
        List<FileMapping> mappings = (List<FileMapping>) value;
        this.fileMappings = mappings != null ? mappings.toArray(new FileMapping[0]) : null;
        this.fileMappingIndex = null; // Invalidate index
        return;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }

  // IndexedRecord implementation (for Avro)
  @Override
  public Schema getSchema() {
    return avroSchema;
  }

  @Override
  public void put(int i, Object v) {
    set(i, v);
  }

  /** Generic implementation of {@link FileMapping}. */
  public static class GenericFileMapping
      implements FileMapping, StructLike, IndexedRecord, SchemaConstructable {
    private transient Schema avroSchema;
    private String sourceFile;
    private String targetFile;
    private Run[] runs;

    public GenericFileMapping(Schema avroSchema) {
      this.avroSchema = avroSchema;
    }

    public GenericFileMapping() {}

    public GenericFileMapping(String sourceFile, String targetFile, List<Run> runs) {
      this.sourceFile = sourceFile;
      this.targetFile = targetFile;
      this.runs = runs.toArray(new Run[0]);
    }

    private GenericFileMapping(GenericFileMapping toCopy) {
      this.sourceFile = toCopy.sourceFile;
      this.targetFile = toCopy.targetFile;
      if (toCopy.runs != null) {
        this.runs = Stream.of(toCopy.runs).map(Run::copy).toArray(Run[]::new);
      }
    }

    @Override
    public String sourceFile() {
      return sourceFile;
    }

    @Override
    public String targetFile() {
      return targetFile;
    }

    @Override
    public List<Run> runs() {
      return runs != null ? ImmutableList.copyOf(runs) : ImmutableList.of();
    }

    @Override
    public Run runForPosition(long sourcePosition) {
      if (runs != null) {
        for (Run run : runs) {
          if (sourcePosition >= run.sourcePosition()
              && sourcePosition < run.sourcePosition() + run.length()) {
            return run;
          }
        }
      }
      return null;
    }

    @Override
    public FileMapping copy() {
      return new GenericFileMapping(this);
    }

    // StructLike implementation
    @Override
    public int size() {
      return 3;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    @Override
    public Object get(int pos) {
      switch (pos) {
        case 0:
          return sourceFile;
        case 1:
          return targetFile;
        case 2:
          return runs != null ? Arrays.asList(runs) : Lists.newArrayList();
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      switch (pos) {
        case 0:
          this.sourceFile = (String) value;
          return;
        case 1:
          this.targetFile = (String) value;
          return;
        case 2:
          @SuppressWarnings("unchecked")
          List<Run> runList = (List<Run>) value;
          this.runs = runList != null ? runList.toArray(new Run[0]) : null;
          return;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    // IndexedRecord implementation (for Avro)
    @Override
    public Schema getSchema() {
      return avroSchema;
    }

    @Override
    public void put(int i, Object v) {
      set(i, v);
    }
  }

  /** Generic implementation of {@link Run}. */
  public static class GenericRun implements Run, StructLike, IndexedRecord, SchemaConstructable {
    private transient Schema avroSchema;
    private long sourcePosition;
    private long targetPosition;
    private long length;

    public GenericRun(Schema avroSchema) {
      this.avroSchema = avroSchema;
    }

    public GenericRun() {}

    public GenericRun(long sourcePosition, long targetPosition, long length) {
      this.sourcePosition = sourcePosition;
      this.targetPosition = targetPosition;
      this.length = length;
    }

    private GenericRun(GenericRun toCopy) {
      this.sourcePosition = toCopy.sourcePosition;
      this.targetPosition = toCopy.targetPosition;
      this.length = toCopy.length;
    }

    @Override
    public long sourcePosition() {
      return sourcePosition;
    }

    @Override
    public long targetPosition() {
      return targetPosition;
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public Run copy() {
      return new GenericRun(this);
    }

    // StructLike implementation
    @Override
    public int size() {
      return 3;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(get(pos));
    }

    @Override
    public Object get(int pos) {
      switch (pos) {
        case 0:
          return sourcePosition;
        case 1:
          return targetPosition;
        case 2:
          return length;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      switch (pos) {
        case 0:
          this.sourcePosition = (Long) value;
          return;
        case 1:
          this.targetPosition = (Long) value;
          return;
        case 2:
          this.length = (Long) value;
          return;
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    // IndexedRecord implementation (for Avro)
    @Override
    public Schema getSchema() {
      return avroSchema;
    }

    @Override
    public void put(int i, Object v) {
      set(i, v);
    }
  }
}
