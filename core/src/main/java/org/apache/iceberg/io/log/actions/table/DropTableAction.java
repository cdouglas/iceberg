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
package org.apache.iceberg.io.log.actions.table;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.log.actions.LogAction;
import org.apache.iceberg.io.log.state.CatalogState;

public class DropTableAction implements LogAction {
  private final int tblId;
  private final int tblVersion;
  private final TableIdentifier tableIdentifier;

  public DropTableAction(int tblId, int tblVersion, TableIdentifier tableIdentifier) {
    this.tblId = tblId;
    this.tblVersion = tblVersion;
    this.tableIdentifier = tableIdentifier;
  }

  @Override
  public boolean verify(CatalogState state) {
    Integer currentVersion = state.tableVersion(tblId);
    if (currentVersion == null) {
      return false;
    }
    return currentVersion.equals(tblVersion);
  }

  @Override
  public void apply(CatalogState.Builder builder) {
    builder.tableBuilder().removeTable(tblId);
  }

  @Override
  public void serialize(DataOutputStream dos) throws IOException {
    dos.writeByte(Type.DROP_TABLE.ordinal());
    dos.writeInt(tblId);
    dos.writeInt(tblVersion);
  }

  @Override
  public Type type() {
    return Type.DROP_TABLE;
  }

  public static DropTableAction deserialize(DataInputStream dis) throws IOException {
    int tblId = dis.readInt();
    int tblVersion = dis.readInt();
    // Note: TableIdentifier needs to be resolved during application
    return new DropTableAction(tblId, tblVersion, null);
  }

  public int tblId() {
    return tblId;
  }

  public int tblVersion() {
    return tblVersion;
  }

  public TableIdentifier tableIdentifier() {
    return tableIdentifier;
  }
}
