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
   * Represents position mapping from a source file to a target file during a rewrite operation.
   *
   * <p>This metadata tracks how rows from source files are mapped to target files, which is used to
   * generate compaction maps for remapping position deletes.
   */
  public static class FilePositionMapping {
    private final String sourceFile;
    private final String targetFile;
    private final long sourceRowCount;
    private final long targetRowCount;
    private final long targetOffset;

    public FilePositionMapping(
        String sourceFile,
        String targetFile,
        long sourceRowCount,
        long targetRowCount,
        long targetOffset) {
      this.sourceFile = sourceFile;
      this.targetFile = targetFile;
      this.sourceRowCount = sourceRowCount;
      this.targetRowCount = targetRowCount;
      this.targetOffset = targetOffset;
    }

    public String sourceFile() {
      return sourceFile;
    }

    public String targetFile() {
      return targetFile;
    }

    public long sourceRowCount() {
      return sourceRowCount;
    }

    public long targetRowCount() {
      return targetRowCount;
    }

    public long targetOffset() {
      return targetOffset;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("sourceFile", sourceFile)
          .add("targetFile", targetFile)
          .add("sourceRowCount", sourceRowCount)
          .add("targetRowCount", targetRowCount)
          .add("targetOffset", targetOffset)
          .toString();
    }
  }
}
