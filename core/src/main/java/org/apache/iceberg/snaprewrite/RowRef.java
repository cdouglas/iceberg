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

import java.util.Objects;

/**
 * A row identified by its data file and position within that file.
 *
 * <p>Snapshot rewriting uses this in two roles: as a key naming a row in the <i>original</i>
 * layout, and as a value naming where that row now lives in the rewritten layout.
 */
public class RowRef {
  private final String path;
  private final long position;

  public RowRef(String path, long position) {
    this.path = path;
    this.position = position;
  }

  public String path() {
    return path;
  }

  public long position() {
    return position;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof RowRef)) {
      return false;
    }

    RowRef that = (RowRef) other;
    return position == that.position && path.equals(that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, position);
  }

  @Override
  public String toString() {
    return path + ":" + position;
  }
}
