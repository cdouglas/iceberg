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
package org.apache.iceberg.actions;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;

/**
 * Container class representing a set of files to be rewritten by a RewriteAction and the new files
 * which have been written by the action.
 */
public class RewriteFileGroup extends RewriteGroupBase<FileGroupInfo, FileScanTask, DataFile> {
  private final int outputSpecId;
  private DataFileSet addedFiles = DataFileSet.create();
  private Map<String, FilePositionMapping> positionMappings = Maps.newHashMap();

  public RewriteFileGroup(
      FileGroupInfo info,
      List<FileScanTask> fileScanTasks,
      int outputSpecId,
      long writeMaxFileSize,
      long inputSplitSize,
      int expectedOutputFiles) {
    super(info, fileScanTasks, writeMaxFileSize, inputSplitSize, expectedOutputFiles);
    this.outputSpecId = outputSpecId;
  }

  public void setOutputFiles(Set<DataFile> files) {
    addedFiles = DataFileSet.of(files);
  }

  public Set<DataFile> rewrittenFiles() {
    return fileScanTasks().stream()
        .map(FileScanTask::file)
        .collect(Collectors.toCollection(DataFileSet::create));
  }

  public Set<DeleteFile> danglingDVs() {
    return fileScanTasks().stream()
        .flatMap(task -> task.deletes().stream().filter(ContentFileUtil::isDV))
        .collect(Collectors.toCollection(DeleteFileSet::create));
  }

  public Set<DataFile> addedFiles() {
    return addedFiles;
  }

  public void setPositionMappings(Map<String, FilePositionMapping> mappings) {
    this.positionMappings = mappings;
  }

  public Map<String, FilePositionMapping> positionMappings() {
    return positionMappings;
  }

  public RewriteDataFiles.FileGroupRewriteResult asResult() {
    Preconditions.checkState(addedFiles != null, "Cannot get result, Group was never rewritten");
    return ImmutableRewriteDataFiles.FileGroupRewriteResult.builder()
        .info(info())
        .addedDataFilesCount(addedFiles.size())
        .rewrittenDataFilesCount(fileScanTasks().size())
        .rewrittenBytesCount(inputFilesSizeInBytes())
        .removedDeleteFilesCount(danglingDVs().size())
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("info", info())
        .add("numRewrittenFiles", fileScanTasks().size())
        .add(
            "numAddedFiles",
            addedFiles == null ? "Rewrite Incomplete" : Integer.toString(addedFiles.size()))
        .add("numRewrittenBytes", inputFilesSizeInBytes())
        .add("maxOutputFileSize", maxOutputFileSize())
        .add("inputSplitSize", inputSplitSize())
        .add("expectedOutputFiles", expectedOutputFiles())
        .add("outputSpecId", outputSpecId)
        .toString();
  }

  public int outputSpecId() {
    return outputSpecId;
  }

  public static Comparator<RewriteFileGroup> comparator(RewriteJobOrder rewriteJobOrder) {
    switch (rewriteJobOrder) {
      case BYTES_ASC:
        return Comparator.comparing(RewriteFileGroup::inputFilesSizeInBytes);
      case BYTES_DESC:
        return Comparator.comparing(
            RewriteFileGroup::inputFilesSizeInBytes, Comparator.reverseOrder());
      case FILES_ASC:
        return Comparator.comparing(RewriteFileGroup::inputFileNum);
      case FILES_DESC:
        return Comparator.comparing(RewriteFileGroup::inputFileNum, Comparator.reverseOrder());
      default:
        return (unused, unused2) -> 0;
    }
  }

  /**
   * Represents position mapping from a source file to target file(s) during a rewrite operation.
   *
   * <p>This metadata tracks how rows from source files are mapped to target files, which is used to
   * generate compaction maps for remapping position deletes.
   *
   * <p>Supports multi-run mappings to handle gaps from deleted rows. Each run represents a
   * contiguous range of positions mapped from source to target.
   *
   * <p>Supports multi-target mappings where a single source file's rows span multiple target files
   * (e.g., due to target file size limits). Each run specifies its own target file.
   */
  public static class FilePositionMapping {
    private final String sourceFile;
    private final String targetFile; // Default target for backward compat; may be null if per-run
    private final List<Run> runs;

    /**
     * Constructor for simple single-run mapping (backward compatible).
     *
     * <p>This is used for simple bin-pack operations without deletes, where all rows from a source
     * file are sequentially written to a target file.
     *
     * @param sourceFile source file path
     * @param targetFile target file path
     * @param sourceRowCount number of rows in source file
     * @param targetRowCount number of rows written to target file (should equal sourceRowCount for
     *     simple mapping)
     * @param targetOffset starting position in target file where these rows are written
     */
    public FilePositionMapping(
        String sourceFile,
        String targetFile,
        long sourceRowCount,
        long targetRowCount,
        long targetOffset) {
      this(
          sourceFile,
          targetFile,
          java.util.Collections.singletonList(new Run(0L, targetOffset, sourceRowCount)));
    }

    /**
     * Constructor for multi-run mapping with gaps.
     *
     * <p>This is used for merge compactions where position deletes cause gaps in the position
     * space. Each run represents a contiguous range of rows mapped from source to target.
     *
     * @param sourceFile source file path
     * @param targetFile target file path
     * @param runs list of position runs
     */
    public FilePositionMapping(String sourceFile, String targetFile, List<Run> runs) {
      this.sourceFile = sourceFile;
      this.targetFile = targetFile;
      this.runs = runs;
    }

    public String sourceFile() {
      return sourceFile;
    }

    public String targetFile() {
      return targetFile;
    }

    /**
     * Returns source row count for backward compatibility.
     *
     * <p>For multi-run mappings, this is the sum of all run lengths.
     */
    public long sourceRowCount() {
      return runs.stream().mapToLong(Run::length).sum();
    }

    /**
     * Returns target row count for backward compatibility.
     *
     * <p>For multi-run mappings, this equals source row count (rows are not duplicated).
     */
    public long targetRowCount() {
      return sourceRowCount();
    }

    /**
     * Returns target offset for backward compatibility.
     *
     * <p>For multi-run mappings, this is the target offset of the first run.
     */
    public long targetOffset() {
      return runs.isEmpty() ? 0L : runs.get(0).targetOffset();
    }

    /** Returns the list of position runs. */
    public List<Run> runs() {
      return runs;
    }

    /**
     * Returns the effective target file for a given run.
     *
     * <p>If the run has its own target file, returns that. Otherwise, returns the mapping's default
     * target file.
     */
    public String targetFileForRun(Run run) {
      return run.targetFile() != null ? run.targetFile() : targetFile;
    }

    /**
     * Returns all unique target files in this mapping.
     *
     * <p>For single-target mappings, returns a set with one element. For multi-target mappings,
     * returns all distinct target files across all runs.
     */
    public Set<String> targetFiles() {
      Set<String> targets =
          runs.stream()
              .map(run -> run.targetFile() != null ? run.targetFile() : targetFile)
              .collect(Collectors.toSet());
      return targets;
    }

    /** Returns true if this mapping spans multiple target files. */
    public boolean isMultiTarget() {
      return targetFiles().size() > 1;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("sourceFile", sourceFile)
          .add("targetFile", targetFile)
          .add("runs", runs)
          .toString();
    }

    /**
     * Represents a contiguous run of rows mapped from source to target.
     *
     * <p>A run describes that rows at positions [sourceOffset, sourceOffset + length) in the source
     * file are mapped to [targetOffset, targetOffset + length) in a target file.
     *
     * <p>The target file can be specified per-run to support multi-target mappings, or null to use
     * the parent FilePositionMapping's targetFile (for backward compatibility).
     */
    public static class Run {
      private final long sourceOffset;
      private final long targetOffset;
      private final long length;
      private final String targetFile; // Per-run target, null means use parent's targetFile

      /** Constructor without per-run target (backward compatible). */
      public Run(long sourceOffset, long targetOffset, long length) {
        this(sourceOffset, targetOffset, length, null);
      }

      /** Constructor with per-run target file for multi-target mappings. */
      public Run(long sourceOffset, long targetOffset, long length, String targetFile) {
        this.sourceOffset = sourceOffset;
        this.targetOffset = targetOffset;
        this.length = length;
        this.targetFile = targetFile;
      }

      public long sourceOffset() {
        return sourceOffset;
      }

      public long targetOffset() {
        return targetOffset;
      }

      public long length() {
        return length;
      }

      /**
       * Returns the target file for this run, or null if the parent's targetFile should be used.
       */
      public String targetFile() {
        return targetFile;
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("sourceOffset", sourceOffset)
            .add("targetOffset", targetOffset)
            .add("length", length)
            .add("targetFile", targetFile)
            .toString();
      }
    }
  }
}
