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
import org.apache.iceberg.view.ViewMetadata;
import org.immutables.value.Value;

/**
 * What a catalog transaction did to a view that existed when it started: replaced or changed it (a
 * new version, properties, location), renamed it, dropped it, or a combination (a dropped view's
 * updates are dropped with it).
 */
@Value.Immutable
public interface ViewChange {
  /** The view, by the name it had when the transaction started. */
  TableIdentifier identifier();

  /**
   * The view's version as the transaction first read it, on which its updates were staged. Every
   * change applies only to this version of the same view (its uuid): a view's metadata is replaced
   * whole, so a change since then fails the commit.
   */
  ViewMetadata base();

  /**
   * The view's metadata after the transaction's updates, or null if it only renamed or dropped it.
   */
  @Nullable
  ViewMetadata updated();

  /** The view's name after the transaction, or null if it keeps its name. */
  @Nullable
  TableIdentifier renamedTo();

  @Value.Default
  default boolean dropped() {
    return false;
  }
}
