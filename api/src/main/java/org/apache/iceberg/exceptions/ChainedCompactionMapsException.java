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
package org.apache.iceberg.exceptions;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CompactionMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Exception thrown when position deletes require chained compaction map composition.
 *
 * <p>This exception is raised when multiple sequential compactions have occurred between a
 * transaction's start and commit time, creating a chain of compaction maps that must be composed to
 * remap position deletes correctly.
 *
 * <p>Example scenario:
 *
 * <pre>
 * 1. Transaction T1 starts at snapshot S1 with deletes for file F1
 * 2. Compaction C1: S1→S2 creates map M1 (F1→F2)
 * 3. Compaction C2: S2→S3 creates map M2 (F2→F3)
 * 4. Transaction T1 tries to commit at S3
 *    → Needs to remap F1→F3, but M1 only maps F1→F2
 *    → M2 maps F2→F3, so maps must be composed: M1 ∘ M2
 * </pre>
 *
 * <p>This is a subtype of {@link CompactionConflictException}, so callers handling compaction
 * conflicts via {@code catch (CompactionConflictException e)} will also catch chained cases. The
 * {@link org.apache.iceberg.PositionDeleteRemapper#fromConflict(CompactionConflictException,
 * org.apache.iceberg.io.FileIO)} method handles both single and chained compactions transparently.
 *
 * <p>When handling manually, applications can either:
 *
 * <ul>
 *   <li>Use the provided compaction maps to compose the chain and remap deletes via {@link
 *       org.apache.iceberg.CompactionMapChain}
 *   <li>Retry the transaction from a more recent snapshot
 * </ul>
 *
 * @see CompactionConflictException for single-map conflicts
 */
public class ChainedCompactionMapsException extends CompactionConflictException {
  private final Set<String> chainedFiles;
  private final List<Long> chainSnapshotIds;
  private final List<CompactionMap> compactionMaps;

  /**
   * Creates a new ChainedCompactionMapsException.
   *
   * @param chainedFiles the set of file paths that require chained mapping
   * @param chainSnapshotIds the snapshot IDs in the chain (oldest to newest)
   * @param compactionMaps the compaction maps in the chain (oldest to newest)
   */
  public ChainedCompactionMapsException(
      Set<String> chainedFiles, List<Long> chainSnapshotIds, List<CompactionMap> compactionMaps) {
    super(
        String.format(
            Locale.ROOT,
            "Cannot commit deletes: %d file(s) require chained compaction map composition. "
                + "Chain spans snapshots %s. Either compose the compaction maps to remap deletes, "
                + "or retry from a more recent snapshot.",
            chainedFiles.size(),
            chainSnapshotIds),
        chainedFiles,
        ImmutableMap.of());
    this.chainedFiles = chainedFiles;
    this.chainSnapshotIds = chainSnapshotIds;
    this.compactionMaps = compactionMaps;
  }

  /**
   * Returns the set of file paths that require chained mapping.
   *
   * <p>These are the original source files from the transaction that were compacted through
   * multiple sequential compaction operations.
   *
   * @return set of file paths requiring chain composition
   */
  public Set<String> chainedFiles() {
    return chainedFiles;
  }

  /**
   * Returns the snapshot IDs in the chain, ordered from oldest to newest.
   *
   * <p>The first snapshot is the source of the first compaction, and the last snapshot is the
   * target of the final compaction.
   *
   * @return list of snapshot IDs in chain order
   */
  public List<Long> chainSnapshotIds() {
    return chainSnapshotIds;
  }

  /**
   * Returns the compaction maps in the chain, ordered from oldest to newest.
   *
   * <p>To remap a position delete through the chain, compose these maps in order: the result of
   * mapping through M1 becomes the input to M2, and so on.
   *
   * @return list of compaction maps in chain order
   */
  public List<CompactionMap> compactionMaps() {
    return compactionMaps;
  }
}
