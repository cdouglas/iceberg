# Compaction Maps Implementation Plan (REVISED)

**REVISION**: Updated to follow Iceberg's Avro serialization pattern (like ManifestFile)
**Target**: Apache Iceberg 1.10.x branch
**Approach**: Alternative 1 - Manifest-Level Metadata (Field ID 521)

---

## Phase 1: Data Structures and Schema (REVISED)

### Task 1.1: Define CompactionMap Schema and Interface

**File**: `api/src/main/java/org/apache/iceberg/CompactionMap.java` (NEW)

Following the ManifestFile pattern, define schema using Types.NestedField:

```java
package org.apache.iceberg;

import java.util.List;
import org.apache.iceberg.types.Types;

/**
 * A compaction map describes how position references in data files
 * are transformed during a compaction operation.
 *
 * <p>This enables concurrent transactions to remap their position
 * deletes when the data layout changes due to compaction.
 *
 * <p>Compaction maps are stored in Avro format and follow the same
 * serialization patterns as ManifestFile.
 */
public interface CompactionMap {

  // Schema field IDs
  int SOURCE_SNAPSHOT_ID_FIELD_ID = 1;
  int TARGET_SNAPSHOT_ID_FIELD_ID = 2;
  int FILE_MAPPINGS_FIELD_ID = 3;
  int FILE_MAPPINGS_ELEMENT_ID = 4;

  // Top-level fields
  Types.NestedField SOURCE_SNAPSHOT_ID =
      Types.NestedField.required(
          SOURCE_SNAPSHOT_ID_FIELD_ID,
          "source_snapshot_id",
          Types.LongType.get(),
          "Snapshot ID before compaction");

  Types.NestedField TARGET_SNAPSHOT_ID =
      Types.NestedField.required(
          TARGET_SNAPSHOT_ID_FIELD_ID,
          "target_snapshot_id",
          Types.LongType.get(),
          "Snapshot ID after compaction");

  Types.NestedField FILE_MAPPINGS =
      Types.NestedField.required(
          FILE_MAPPINGS_FIELD_ID,
          "file_mappings",
          Types.ListType.ofRequired(FILE_MAPPINGS_ELEMENT_ID, fileMappingType()),
          "List of file mappings in this compaction");

  // FileMapping struct type
  static Types.StructType fileMappingType() {
    return Types.StructType.of(
        Types.NestedField.required(5, "source_file", Types.StringType.get(),
            "Source file path (pre-compaction)"),
        Types.NestedField.required(6, "target_file", Types.StringType.get(),
            "Target file path (post-compaction)"),
        Types.NestedField.required(7, "runs", Types.ListType.ofRequired(8, runType()),
            "List of position mapping runs")
    );
  }

  // Run struct type
  static Types.StructType runType() {
    return Types.StructType.of(
        Types.NestedField.required(9, "source_position", Types.LongType.get(),
            "Starting position in source file"),
        Types.NestedField.required(10, "target_position", Types.LongType.get(),
            "Starting position in target file"),
        Types.NestedField.required(11, "length", Types.LongType.get(),
            "Number of rows in this run")
    );
  }

  // Schema for the compaction map file
  Schema SCHEMA = new Schema(SOURCE_SNAPSHOT_ID, TARGET_SNAPSHOT_ID, FILE_MAPPINGS);

  static Schema schema() {
    return SCHEMA;
  }

  /** Returns the snapshot ID before compaction. */
  long sourceSnapshotId();

  /** Returns the snapshot ID after compaction. */
  long targetSnapshotId();

  /** Returns the list of file mappings in this compaction. */
  List<FileMapping> fileMappings();

  /**
   * Returns the mapping for a specific source file path, or null if not found.
   */
  FileMapping mappingForFile(String sourceFilePath);

  /**
   * Copies this {@link CompactionMap}. Readers can reuse instances; use this
   * method to make defensive copies.
   *
   * @return a copy of this compaction map
   */
  CompactionMap copy();

  /**
   * Represents the mapping for a single data file that was compacted.
   */
  interface FileMapping {
    /** Source file path (pre-compaction). */
    String sourceFile();

    /** Target file path (post-compaction). */
    String targetFile();

    /** List of position mapping runs. */
    List<Run> runs();

    /**
     * Returns the Run containing the given source position, or null if not found.
     */
    Run runForPosition(long sourcePosition);

    /**
     * Copies this {@link FileMapping}.
     */
    FileMapping copy();
  }

  /**
   * Represents a contiguous run of rows mapped from source to target.
   *
   * <p>A run describes that rows at positions [sourcePosition, sourcePosition + length)
   * in the source file are mapped to [targetPosition, targetPosition + length)
   * in the target file.
   */
  interface Run {
    /** Starting position in source file. */
    long sourcePosition();

    /** Starting position in target file. */
    long targetPosition();

    /** Number of rows in this run. */
    long length();

    /**
     * Given a position in the source file (must be within this run),
     * returns the corresponding position in the target file.
     */
    default long mapPosition(long sourcePos) {
      if (sourcePos < sourcePosition() || sourcePos >= sourcePosition() + length()) {
        throw new IllegalArgumentException(
            String.format("Position %d is not within run [%d, %d)",
                sourcePos, sourcePosition(), sourcePosition() + length()));
      }
      return targetPosition() + (sourcePos - sourcePosition());
    }

    /**
     * Copies this {@link Run}.
     */
    Run copy();
  }
}
```

---

### Task 1.2: Implement GenericCompactionMap (like GenericManifestFile)

**File**: `core/src/main/java/org/apache/iceberg/GenericCompactionMap.java` (NEW)

```java
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
    super(
        CompactionMap.schema().asStruct(),
        AvroSchemaUtil.convert(avroSchema).asStructType());
    this.avroSchema = avroSchema;
  }

  /** Used by Avro reflection to instantiate this class when reading compaction map files. */
  GenericCompactionMap(Types.StructType projectedSchema) {
    super(CompactionMap.schema().asStruct(), projectedSchema);
    this.avroSchema = AVRO_SCHEMA;
  }

  /** Constructor for programmatic creation. */
  public GenericCompactionMap(
      long sourceSnapshotId,
      long targetSnapshotId,
      List<FileMapping> fileMappings) {
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
          Stream.of(toCopy.fileMappings)
              .map(FileMapping::copy)
              .toArray(FileMapping[]::new);
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
  public int size() {
    return CompactionMap.schema().columns().size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(get(pos));
  }

  @Override
  public Object get(int i) {
    int pos = i;
    // Adjust for projection if needed
    if (isMapped(pos)) {
      pos = mapPosition(pos);
    }

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
  public <T> void set(int pos, T value) {
    if (isMapped(pos)) {
      pos = mapPosition(pos);
    }

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

  // SchemaConstructable implementation
  // (Avro uses this to construct instances with the schema)

  /**
   * Generic implementation of {@link FileMapping}.
   */
  public static class GenericFileMapping implements FileMapping, StructLike {
    private String sourceFile;
    private String targetFile;
    private Run[] runs;

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
          if (sourcePosition >= run.sourcePosition() &&
              sourcePosition < run.sourcePosition() + run.length()) {
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
  }

  /**
   * Generic implementation of {@link Run}.
   */
  public static class GenericRun implements Run, StructLike {
    private long sourcePosition;
    private long targetPosition;
    private long length;

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
  }
}
```

---

### Task 1.3: Implement CompactionMaps Utility (like ManifestLists)

**File**: `core/src/main/java/org/apache/iceberg/CompactionMaps.java` (NEW)

```java
package org.apache.iceberg;

import java.io.IOException;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * Utility class for reading and writing compaction maps in Avro format.
 *
 * <p>This follows the same pattern as {@link ManifestLists}.
 */
class CompactionMaps {
  private CompactionMaps() {}

  /**
   * Reads a compaction map from an Avro file.
   *
   * @param inputFile the input file to read
   * @return the compaction map
   */
  static CompactionMap read(InputFile inputFile) {
    try (CloseableIterable<CompactionMap> maps =
        Avro.read(inputFile)
            .rename("compaction_map", GenericCompactionMap.class.getName())
            .rename("file_mappings.element", GenericCompactionMap.GenericFileMapping.class.getName())
            .rename("runs.element", GenericCompactionMap.GenericRun.class.getName())
            .classLoader(GenericCompactionMap.class.getClassLoader())
            .project(CompactionMap.schema())
            .reuseContainers(false)
            .build()) {

      // Compaction map file should contain exactly one record
      return maps.iterator().next();

    } catch (IOException e) {
      throw new RuntimeIOException(
          e, "Cannot read compaction map file: %s", inputFile.location());
    }
  }

  /**
   * Creates a writer for compaction maps.
   *
   * @param outputFile the output file to write to
   * @return a writer for compaction maps
   */
  static CompactionMapWriter write(OutputFile outputFile) {
    return new CompactionMapWriter(outputFile);
  }

  /**
   * Writer for compaction map files.
   */
  static class CompactionMapWriter implements java.io.Closeable {
    private final OutputFile outputFile;
    private org.apache.iceberg.io.FileAppender<CompactionMap> writer;

    CompactionMapWriter(OutputFile outputFile) {
      this.outputFile = outputFile;
    }

    /**
     * Writes a compaction map to the file.
     *
     * <p>Only one compaction map should be written per file.
     *
     * @param map the compaction map to write
     */
    void write(CompactionMap map) {
      if (writer == null) {
        this.writer =
            Avro.write(outputFile)
                .schema(CompactionMap.schema())
                .named("compaction_map")
                .overwrite()
                .build();
      }

      writer.add(map);
    }

    @Override
    public void close() throws IOException {
      if (writer != null) {
        writer.close();
      }
    }
  }
}
```

---

### Task 1.4: Update CompactionMapBuilder to Use Generic Classes

**File**: `core/src/main/java/org/apache/iceberg/CompactionMapBuilder.java`

**Changes**:

```java
public class CompactionMapBuilder {
  private final long sourceSnapshotId;
  private final long targetSnapshotId;
  private final Map<String, FileMappingBuilder> fileMappings = new HashMap<>();

  // ... existing methods ...

  public CompactionMap build() {
    List<CompactionMap.FileMapping> mappings = new ArrayList<>();

    for (FileMappingBuilder mappingBuilder : fileMappings.values()) {
      mappings.add(mappingBuilder.build());
    }

    // Use GenericCompactionMap instead of BaseCompactionMap
    return new GenericCompactionMap(sourceSnapshotId, targetSnapshotId, mappings);
  }

  private static class FileMappingBuilder {
    // ... existing code ...

    CompactionMap.FileMapping build() {
      // ... consolidate into runs as before ...

      // Use GenericFileMapping and GenericRun
      List<CompactionMap.Run> runs = new ArrayList<>();
      // ... build runs ...
      for (...) {
        runs.add(new GenericCompactionMap.GenericRun(runStartSource, runStartTarget, runLength));
      }

      return new GenericCompactionMap.GenericFileMapping(sourceFile, targetFile, runs);
    }
  }
}
```

---

## Phase 3: Map Storage and Retrieval (REVISED)

### Task 3.2: Update CompactionMapWriter Usage

**File**: Wherever maps are written (e.g., in RewriteDataFilesSparkAction)

**Old approach** (from original plan):
```java
CompactionMapWriter.write(map, output);
```

**New approach** (following Iceberg pattern):
```java
CompactionMaps.CompactionMapWriter writer = CompactionMaps.write(output);
try {
  writer.write(map);
} finally {
  writer.close();
}

// Or more idiomatically:
try (CompactionMaps.CompactionMapWriter writer = CompactionMaps.write(output)) {
  writer.write(map);
}
```

---

### Task 3.3: Update CompactionMapReader Usage

**Old approach**:
```java
CompactionMap map = CompactionMapReader.read(input);
```

**New approach**:
```java
CompactionMap map = CompactionMaps.read(input);
```

---

## Updated Test Examples

### Test: CompactionMap Serialization (REVISED)

**File**: `core/src/test/java/org/apache/iceberg/TestCompactionMapSerialization.java`

```java
public class TestCompactionMapSerialization extends TestBase {

  @Test
  public void testRoundTrip() throws IOException {
    // Create compaction map using Generic classes
    CompactionMap.Run run1 = new GenericCompactionMap.GenericRun(0, 0, 100);
    CompactionMap.Run run2 = new GenericCompactionMap.GenericRun(200, 100, 50);

    CompactionMap.FileMapping mapping = new GenericCompactionMap.GenericFileMapping(
        "source.parquet",
        "target.parquet",
        Lists.newArrayList(run1, run2));

    CompactionMap originalMap = new GenericCompactionMap(
        123L,  // sourceSnapshotId
        456L,  // targetSnapshotId
        Lists.newArrayList(mapping));

    // Write to file using CompactionMaps utility
    OutputFile output = Files.localOutput(temp.newFile());
    try (CompactionMaps.CompactionMapWriter writer = CompactionMaps.write(output)) {
      writer.write(originalMap);
    }

    // Read back using CompactionMaps utility
    InputFile input = Files.localInput(output.location());
    CompactionMap loadedMap = CompactionMaps.read(input);

    // Verify
    assertEquals(originalMap.sourceSnapshotId(), loadedMap.sourceSnapshotId());
    assertEquals(originalMap.targetSnapshotId(), loadedMap.targetSnapshotId());
    assertEquals(1, loadedMap.fileMappings().size());

    CompactionMap.FileMapping loadedMapping = loadedMap.fileMappings().get(0);
    assertEquals("source.parquet", loadedMapping.sourceFile());
    assertEquals("target.parquet", loadedMapping.targetFile());
    assertEquals(2, loadedMapping.runs().size());

    CompactionMap.Run loadedRun1 = loadedMapping.runs().get(0);
    assertEquals(0, loadedRun1.sourcePosition());
    assertEquals(0, loadedRun1.targetPosition());
    assertEquals(100, loadedRun1.length());
  }

  @Test
  public void testAvroSchema() {
    // Verify that Avro schema is correctly generated
    org.apache.avro.Schema avroSchema =
        org.apache.iceberg.avro.AvroSchemaUtil.convert(
            CompactionMap.schema(), "compaction_map");

    assertNotNull(avroSchema);
    assertEquals("compaction_map", avroSchema.getName());

    // Verify fields
    assertNotNull(avroSchema.getField("source_snapshot_id"));
    assertNotNull(avroSchema.getField("target_snapshot_id"));
    assertNotNull(avroSchema.getField("file_mappings"));

    // Verify nested structures
    org.apache.avro.Schema fileMappingsType = avroSchema.getField("file_mappings").schema();
    assertTrue(fileMappingsType.getType() == org.apache.avro.Schema.Type.ARRAY);
  }

  @Test
  public void testSchemaEvolution() throws IOException {
    // Test that adding optional fields to schema doesn't break compatibility
    // (similar to how ManifestFile handles backward compatibility)

    // Create map with current schema
    CompactionMap originalMap = createTestMap();

    OutputFile output = Files.localOutput(temp.newFile());
    try (CompactionMaps.CompactionMapWriter writer = CompactionMaps.write(output)) {
      writer.write(originalMap);
    }

    // Read with potentially evolved schema (would have additional optional fields)
    InputFile input = Files.localInput(output.location());
    CompactionMap loadedMap = CompactionMaps.read(input);

    assertNotNull(loadedMap);
    assertEquals(originalMap.sourceSnapshotId(), loadedMap.sourceSnapshotId());
  }
}
```

---

## Updated Integration Points

### Phase 4: Transaction Integration (REVISED)

**File**: `core/src/main/java/org/apache/iceberg/PositionDeleteRemapper.java`

**Changes**:

```java
public class PositionDeleteRemapper {
  private final FileIO fileIO;
  private final Map<String, CompactionMap> compactionMaps;

  // ... constructor ...

  public void loadCompactionMap(String mapLocation) throws IOException {
    InputFile input = fileIO.newInputFile(mapLocation);

    // Use CompactionMaps utility (like ManifestLists.read)
    CompactionMap map = CompactionMaps.read(input);

    // Index by source file for fast lookup
    for (CompactionMap.FileMapping mapping : map.fileMappings()) {
      compactionMaps.put(mapping.sourceFile(), map);
    }
  }

  // ... rest of implementation unchanged ...
}
```

---

### Phase 5: Compaction Integration (REVISED)

**File**: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java`

**Changes**:

```java
private String storeCompactionMap(CompactionMap map) throws IOException {
  String mapLocation = compactionMapLocation(table, map.targetSnapshotId());
  OutputFile output = table.io().newOutputFile(mapLocation);

  // Use CompactionMaps utility (like ManifestLists.write)
  try (CompactionMaps.CompactionMapWriter writer = CompactionMaps.write(output)) {
    writer.write(map);
  }

  LOG.info("Wrote compaction map to {}", mapLocation);
  return mapLocation;
}
```

---

## Benefits of This Approach

### 1. **Consistency with Iceberg Patterns**
- Follows the exact pattern used by ManifestFile/ManifestLists
- Uses Avro for serialization (not Java Serialization)
- Implements StructLike, IndexedRecord for Avro compatibility
- Uses Types.NestedField for schema definition

### 2. **Schema Evolution Support**
- Can add optional fields in the future without breaking compatibility
- Avro's schema evolution handles backward/forward compatibility
- Same guarantees as ManifestFile schema evolution

### 3. **Better Integration**
- Works seamlessly with Iceberg's Avro infrastructure
- Can leverage Avro's compression (Snappy, etc.)
- Compatible with Iceberg's projection and filtering
- Easier to debug with standard Avro tools

### 4. **Type Safety**
- Still maintains Java type safety through interfaces
- Generic implementations handle Avro serialization
- No manual Encoder/Decoder writing needed

### 5. **Performance**
- Avro's binary format is efficient
- Built-in compression support
- Schema is embedded in files (self-describing)

---

## Comparison: Old vs New Approach

### Old Approach (from original plan)
```java
// Interface extended Serializable (not Iceberg pattern)
public interface CompactionMap extends Serializable { ... }

// Manual Avro writer
private static class CompactionMapAvroWriter
    implements org.apache.iceberg.avro.ValueWriter<CompactionMap> {
  @Override
  public void write(CompactionMap map, Encoder encoder) throws IOException {
    encoder.writeLong(map.sourceSnapshotId());
    encoder.writeLong(map.targetSnapshotId());
    // Manual encoding...
  }
}

// Manual Avro reader
private static class CompactionMapAvroReader
    implements org.apache.iceberg.avro.ValueReader<CompactionMap> {
  @Override
  public CompactionMap read(Decoder decoder, Object reuse) throws IOException {
    long sourceSnapshotId = decoder.readLong();
    long targetSnapshotId = decoder.readLong();
    // Manual decoding...
  }
}
```

### New Approach (following Iceberg pattern)
```java
// Interface defines schema with Types.NestedField (Iceberg pattern)
public interface CompactionMap {
  Types.NestedField SOURCE_SNAPSHOT_ID = ...;
  Schema SCHEMA = new Schema(...);
}

// Generic implementation handles Avro automatically
public class GenericCompactionMap extends SupportsIndexProjection
    implements CompactionMap, StructLike, IndexedRecord {
  // Implements StructLike.get() and .set() - Avro handles the rest
}

// Utility class like ManifestLists
class CompactionMaps {
  static CompactionMap read(InputFile inputFile) {
    return Avro.read(inputFile).project(CompactionMap.schema()).build();
  }

  static CompactionMapWriter write(OutputFile outputFile) { ... }
}
```

---

## Updated Implementation Checklist

### Phase 1: Data Structures and Schema (REVISED)
- [x] Task 1.1: CompactionMap interface with Types.NestedField schema
- [x] Task 1.2: GenericCompactionMap (like GenericManifestFile)
- [x] Task 1.3: CompactionMaps utility (like ManifestLists)
- [x] Task 1.4: Update CompactionMapBuilder to use Generic classes
- [ ] Task 1.5: ManifestFile schema extension (unchanged)

### Phase 2: Map Generation (UNCHANGED)
- Same as original plan

### Phase 3: Storage and Retrieval (REVISED)
- [x] Task 3.1: Schema defined via Types.NestedField (done in Phase 1)
- [x] Task 3.2: Use CompactionMaps.write() instead of manual writer
- [x] Task 3.3: Use CompactionMaps.read() instead of manual reader
- [ ] Task 3.4: Table properties (unchanged)

### Phase 4-8: (Minor updates to use new API)
- All subsequent phases use `CompactionMaps.read()` and `CompactionMaps.write()`
- No other changes to logic or tests needed

---

## Migration Notes

### Changes from Original Plan

1. **Removed**: Custom `CompactionMapAvroWriter` and `CompactionMapAvroReader`
   - **Reason**: Iceberg's Avro infrastructure handles this automatically via StructLike

2. **Removed**: `BaseCompactionMap.Builder` class
   - **Replaced with**: Direct construction of `GenericCompactionMap`

3. **Added**: `SupportsIndexProjection` base class
   - **Reason**: Enables projection support like ManifestFile

4. **Changed**: Writer/Reader API
   - **Old**: `CompactionMapWriter.write(map, output)`
   - **New**: `try (CompactionMaps.CompactionMapWriter w = CompactionMaps.write(out)) { w.write(map); }`

### Files to Update

- `CompactionMap.java` - Define schema with Types.NestedField
- `GenericCompactionMap.java` - NEW (replaces BaseCompactionMap)
- `CompactionMaps.java` - NEW (replaces CompactionMapWriter/Reader)
- `CompactionMapBuilder.java` - Update to use GenericCompactionMap
- All integration points - Use new CompactionMaps API

---

## Conclusion

This revised approach:
- ✅ Follows Iceberg's established patterns (ManifestFile/ManifestLists)
- ✅ Uses Avro for serialization (not Java Serialization)
- ✅ Leverages Iceberg's Avro infrastructure automatically
- ✅ Supports schema evolution like other Iceberg metadata
- ✅ Maintains type safety and clean interfaces
- ✅ Integrates seamlessly with existing code

The implementation is now consistent with how Iceberg handles all its metadata structures.
