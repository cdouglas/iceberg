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
package org.apache.iceberg.snaprewrite;

/**
 * A reason a window cannot be rewritten losslessly.
 *
 * <p>Every one of these aborts the whole window rather than degrading. A rewrite that is lossless
 * for most snapshots and lossy for one still silently changes what a time-travel read returns, so
 * partial success is not a useful outcome.
 */
public enum RewriteRefusal {
  FORMAT_VERSION("table format version is not 2"),
  EQUALITY_DELETES("window contains equality deletes"),
  SCHEMA_CHANGED("schema changed across the window"),
  SPEC_CHANGED("partition spec changed across the window"),
  MISSING_COMPACTION_MAP("a replace operation in the window has no compaction map"),
  MISSING_SOURCE_FILE("a data file needed for resurrection no longer exists"),
  TOO_RECENT("the compaction is newer than the configured minimum age"),
  DEAD_RATIO("too little would be reclaimed relative to the rows to be rewritten"),
  UNLOCATABLE_ROW("a row live in a rewritten snapshot could not be located"),
  NO_COMPACTION("no compaction with a compaction map was found");

  private final String description;

  RewriteRefusal(String description) {
    this.description = description;
  }

  public String description() {
    return description;
  }
}
