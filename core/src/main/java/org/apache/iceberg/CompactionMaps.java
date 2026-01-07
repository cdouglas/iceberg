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
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
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
        InternalData.read(FileFormat.AVRO, inputFile)
            .setRootType(GenericCompactionMap.class)
            .setCustomType(
                CompactionMap.FILE_MAPPINGS_ELEMENT_ID,
                GenericCompactionMap.GenericFileMapping.class)
            .setCustomType(8, GenericCompactionMap.GenericRun.class)
            .project(CompactionMap.schema())
            .build()) {

      // Compaction map file should contain exactly one record
      return maps.iterator().next();

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Cannot read compaction map file: %s", inputFile.location());
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

  /** Writer for compaction map files. */
  static class CompactionMapWriter implements java.io.Closeable {
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
    void write(CompactionMap map) throws IOException {
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
