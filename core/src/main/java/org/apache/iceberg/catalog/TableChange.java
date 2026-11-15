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
package org.apache.iceberg.catalog;

import javax.annotation.Nullable;
import org.apache.iceberg.TableMetadata;
import org.immutables.value.Value;

/**
 * What a catalog transaction did to a table that existed when it started: updated it, renamed it,
 * dropped it, or a combination (a dropped table's updates are dropped with it).
 */
@Value.Immutable
public interface TableChange {
  /** The table, by the name it had when the transaction started. */
  TableIdentifier identifier();

  /**
   * The table's version as the transaction first read it, on which its updates were staged. A drop
   * or rename applies only to the same table (its uuid); updates are judged from this version.
   */
  TableMetadata base();

  /** The table's updates, or null if the transaction only renamed or dropped it. */
  @Nullable
  TableCommit commit();

  /** The table's name after the transaction, or null if it keeps its name. */
  @Nullable
  TableIdentifier renamedTo();

  @Value.Default
  default boolean dropped() {
    return false;
  }

  /** Whether a drop also deletes the table's files, after the commit lands. */
  @Value.Default
  default boolean purge() {
    return false;
  }
}
