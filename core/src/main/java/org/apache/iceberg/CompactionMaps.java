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

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Utility class for reading and writing compaction maps in Avro format.
 *
 * <p>This follows the same pattern as {@link ManifestLists}.
 */
public class CompactionMaps {
  private CompactionMaps() {}

  /**
   * Generates a new compaction map file location for the given snapshot.
   *
   * <p>The file will be located in the table's metadata directory following the pattern: {@code
   * compaction-map-<snapshotId>-<uuid>.avro}
   *
   * @param table the table for which to generate a compaction map file location
   * @param snapshotId the snapshot ID for which the compaction map is being created
   * @return an output file for the compaction map
   */
  public static OutputFile newCompactionMapFile(Table table, long snapshotId) {
    Preconditions.checkArgument(
        table instanceof HasTableOperations,
        "Table must have operations to retrieve metadata location");

    String fileName =
        String.format(
            Locale.ROOT,
            "compaction-map-%d-%s%s",
            snapshotId,
            UUID.randomUUID(),
            FileFormat.AVRO.addExtension(""));

    return table
        .io()
        .newOutputFile(((HasTableOperations) table).operations().metadataFileLocation(fileName));
  }

  /**
   * Reads a compaction map from an Avro file.
   *
   * @param inputFile the input file to read
   * @return the compaction map
   */
  public static CompactionMap read(InputFile inputFile) {
    try (CloseableIterable<CompactionMap> maps =
        InternalData.read(FileFormat.AVRO, inputFile)
            .setRootType(GenericCompactionMap.class)
            .setCustomType(
                CompactionMap.FILE_MAPPINGS_ELEMENT_ID,
                GenericCompactionMap.GenericFileMapping.class)
            .setCustomType(8, GenericCompactionMap.GenericRun.class)
            .project(CompactionMap.schema())
            .build()) {

      // Compaction map file should contain exactly one record
      CompactionMap map = maps.iterator().next();

      // Intern target paths to reduce memory usage when many sources map to same target
      if (map instanceof GenericCompactionMap) {
        ((GenericCompactionMap) map).internTargetPaths();
      }

      return map;

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Cannot read compaction map file: %s", inputFile.location());
    }
  }

  /**
   * Serializes a compaction map to a byte array using Avro encoding.
   *
   * <p>This is useful for embedding compaction maps in Java-serializable closures (e.g., Spark map
   * functions) where the map data must survive serialization to executors.
   *
   * @param map the compaction map to serialize
   * @return the Avro-encoded bytes
   */
  public static byte[] toBytes(CompactionMap map) {
    org.apache.iceberg.inmemory.InMemoryOutputFile output =
        new org.apache.iceberg.inmemory.InMemoryOutputFile();
    try {
      write(map, output);
      return output.toByteArray();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to serialize compaction map to bytes");
    }
  }

  /**
   * Deserializes a compaction map from Avro-encoded bytes produced by {@link #toBytes(CompactionMap)}.
   *
   * @param bytes the Avro-encoded bytes
   * @return the deserialized compaction map
   */
  public static CompactionMap fromBytes(byte[] bytes) {
    Preconditions.checkNotNull(bytes, "bytes cannot be null");
    org.apache.iceberg.inmemory.InMemoryInputFile input =
        new org.apache.iceberg.inmemory.InMemoryInputFile(bytes);
    return read(input);
  }

  /**
   * Creates a writer for compaction maps.
   *
   * @param outputFile the output file to write to
   * @return a writer for compaction maps
   */
  public static CompactionMapWriter write(OutputFile outputFile) {
    return new CompactionMapWriter(outputFile);
  }

  /**
   * Convenience method to write a compaction map to a file.
   *
   * @param map the compaction map to write
   * @param outputFile the output file to write to
   * @throws IOException if an error occurs during writing
   */
  public static void write(CompactionMap map, OutputFile outputFile) throws IOException {
    try (CompactionMapWriter writer = write(outputFile)) {
      writer.write(map);
    }
  }

  /**
   * Composes two compaction maps into a single map representing the combined transformation.
   *
   * <p>Given map M1 (sourceSnapshot1 → targetSnapshot1) and map M2 (sourceSnapshot2 →
   * targetSnapshot2), where M1's target snapshot equals M2's source snapshot, this method produces
   * a composed map M' (sourceSnapshot1 → targetSnapshot2) that directly maps source positions in M1
   * to target positions in M2.
   *
   * <p>For example, if M1 maps F1→F2 and M2 maps F2→F3, the composed map will map F1→F3 directly.
   *
   * <p><b>Composition Algorithm:</b>
   *
   * <pre>
   * For each file mapping (F1 → F2) in M1:
   *   If F2 is a source in M2 (F2 → F3):
   *     For each run r1 in M1's mapping:
   *       For each run r2 in M2's mapping for F2:
   *         Compute overlap between r1's target range and r2's source range
   *         If overlap exists:
   *           Create composed run: F1[overlap_start] → F3[r2.mapPosition(overlap_start)]
   *   Else:
   *     Copy M1's mapping unchanged (F2 is the final target)
   * </pre>
   *
   * @param m1 the first compaction map (applied first in the chain)
   * @param m2 the second compaction map (applied second in the chain)
   * @return a composed compaction map representing the combined transformation
   * @throws IllegalArgumentException if m1's target snapshot doesn't match m2's source snapshot
   */
  public static CompactionMap compose(CompactionMap m1, CompactionMap m2) {
    Preconditions.checkArgument(
        m1.targetSnapshotId() == m2.sourceSnapshotId(),
        "Cannot compose maps: m1.targetSnapshotId (%s) != m2.sourceSnapshotId (%s)",
        m1.targetSnapshotId(),
        m2.sourceSnapshotId());

    // Build index from source file to mapping in m2
    Map<String, CompactionMap.FileMapping> m2SourceIndex = Maps.newHashMap();
    for (CompactionMap.FileMapping mapping : m2.fileMappings()) {
      m2SourceIndex.put(mapping.sourceFile(), mapping);
    }

    CompactionMapBuilder builder =
        new CompactionMapBuilder(m1.sourceSnapshotId(), m2.targetSnapshotId());

    for (CompactionMap.FileMapping m1Mapping : m1.fileMappings()) {
      // Get all target files from m1's mapping (may be multiple for multi-target)
      Map<String, List<CompactionMap.Run>> runsByTarget = groupRunsByTarget(m1Mapping);

      for (Map.Entry<String, List<CompactionMap.Run>> entry : runsByTarget.entrySet()) {
        String intermediateFile = entry.getKey();
        List<CompactionMap.Run> m1Runs = entry.getValue();

        // Check if the intermediate file is a source in m2
        CompactionMap.FileMapping m2Mapping = m2SourceIndex.get(intermediateFile);

        if (m2Mapping == null) {
          // Intermediate file is not compacted further - copy runs as-is
          // but need to reference the intermediate file as the target
          copyRunsToBuilder(builder, m1Mapping.sourceFile(), intermediateFile, m1Runs);
        } else {
          // Compose through m2. Only positions that overlap with m2's runs will be mapped
          // through to the final target. Non-overlapping positions are dropped since they
          // reference rows that will be compacted away.
          composeFileMappings(builder, m1Mapping.sourceFile(), intermediateFile, m1Runs, m2Mapping);
        }
      }
    }

    return builder.build();
  }

  /**
   * Groups runs by their effective target file.
   *
   * @param mapping the file mapping
   * @return map from target file to list of runs targeting that file
   */
  private static Map<String, List<CompactionMap.Run>> groupRunsByTarget(
      CompactionMap.FileMapping mapping) {
    Map<String, List<CompactionMap.Run>> result = Maps.newLinkedHashMap();

    for (CompactionMap.Run run : mapping.runs()) {
      String target = run.targetFile() != null ? run.targetFile() : mapping.targetFile();
      result.computeIfAbsent(target, k -> Lists.newArrayList()).add(run);
    }

    return result;
  }

  /**
   * Copies runs to the builder unchanged.
   *
   * @param builder the builder to add runs to
   * @param sourceFile the original source file
   * @param targetFile the target file
   * @param runs the runs to copy
   */
  private static void copyRunsToBuilder(
      CompactionMapBuilder builder,
      String sourceFile,
      String targetFile,
      List<CompactionMap.Run> runs) {

    CompactionMapBuilder.FileMappingBuilder fileMappingBuilder =
        getOrCreateFileMapping(builder, sourceFile, targetFile);

    for (CompactionMap.Run run : runs) {
      fileMappingBuilder.addRun(
          run.sourcePosition(), run.targetPosition(), run.length(), run.targetFile());
    }
  }

  /**
   * Composes file mappings from m1 through m2.
   *
   * <p>For each run in m1, this method finds overlapping runs in m2 and creates composed runs. Runs
   * that have no overlap with any m2 run are preserved with the intermediate file as target (since
   * those rows weren't part of the M2 compaction). Runs that have partial overlap will have only
   * the overlapping portion composed; non-overlapping portions are dropped.
   *
   * @param builder the builder to add composed runs to
   * @param originalSourceFile the original source file in m1
   * @param intermediateFile the intermediate file (m1's target / m2's source)
   * @param m1Runs the runs from m1
   * @param m2Mapping the file mapping in m2
   */
  private static void composeFileMappings(
      CompactionMapBuilder builder,
      String originalSourceFile,
      String intermediateFile,
      List<CompactionMap.Run> m1Runs,
      CompactionMap.FileMapping m2Mapping) {

    for (CompactionMap.Run r1 : m1Runs) {
      long r1TargetStart = r1.targetPosition();
      long r1TargetEnd = r1TargetStart + r1.length();
      boolean anyOverlap = false;

      // Find overlapping runs in m2
      for (CompactionMap.Run r2 : m2Mapping.runs()) {
        long r2SourceStart = r2.sourcePosition();
        long r2SourceEnd = r2SourceStart + r2.length();

        // Check for overlap between r1's target range and r2's source range
        long overlapStart = Math.max(r1TargetStart, r2SourceStart);
        long overlapEnd = Math.min(r1TargetEnd, r2SourceEnd);

        if (overlapStart < overlapEnd) {
          anyOverlap = true;
          // There's an overlap - create composed run
          long overlapLength = overlapEnd - overlapStart;

          // Compute source position in original file:
          // r1 maps [r1.sourcePosition, r1.sourcePosition + length) ->
          //         [r1.targetPosition, r1.targetPosition + length)
          // overlapStart is a position in r1's target range
          // corresponding source position = r1.sourcePosition + (overlapStart - r1.targetPosition)
          long composedSourcePos = r1.sourcePosition() + (overlapStart - r1.targetPosition());

          // Compute target position in final file:
          // r2 maps [r2.sourcePosition, r2.sourcePosition + length) ->
          //         [r2.targetPosition, r2.targetPosition + length)
          // overlapStart is a position in r2's source range
          // corresponding target position = r2.targetPosition + (overlapStart - r2.sourcePosition)
          long composedTargetPos = r2.targetPosition() + (overlapStart - r2.sourcePosition());

          // Get the final target file
          String finalTargetFile =
              r2.targetFile() != null ? r2.targetFile() : m2Mapping.targetFile();

          // Add composed run to builder
          CompactionMapBuilder.FileMappingBuilder fileMappingBuilder =
              getOrCreateFileMapping(builder, originalSourceFile, finalTargetFile);

          fileMappingBuilder.addRun(
              composedSourcePos, composedTargetPos, overlapLength, finalTargetFile);
        }
      }

      // If no overlap was found, preserve the original mapping to the intermediate file.
      // This handles the case where M2 compacts different rows of the intermediate file
      // than what M1 maps to.
      if (!anyOverlap) {
        CompactionMapBuilder.FileMappingBuilder fileMappingBuilder =
            getOrCreateFileMapping(builder, originalSourceFile, intermediateFile);
        fileMappingBuilder.addRun(
            r1.sourcePosition(), r1.targetPosition(), r1.length(), r1.targetFile());
      }
    }
  }

  /**
   * Gets or creates a file mapping builder for the given source/target pair.
   *
   * @param builder the compaction map builder
   * @param sourceFile the source file
   * @param targetFile the target file
   * @return the file mapping builder
   */
  private static CompactionMapBuilder.FileMappingBuilder getOrCreateFileMapping(
      CompactionMapBuilder builder, String sourceFile, String targetFile) {
    CompactionMapBuilder.FileMappingBuilder existing = builder.getFileMapping(sourceFile);
    if (existing != null) {
      return existing;
    }
    return builder.addFileMapping(sourceFile, targetFile);
  }

  /** Writer for compaction map files. */
  public static class CompactionMapWriter implements java.io.Closeable {
    private final OutputFile outputFile;
    private FileAppender<CompactionMap> writer;

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
    public void write(CompactionMap map) throws IOException {
      if (writer == null) {
        this.writer =
            Avro.write(outputFile).schema(CompactionMap.schema()).named("compaction_map").build();
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
