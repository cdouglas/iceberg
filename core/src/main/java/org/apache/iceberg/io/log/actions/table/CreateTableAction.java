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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.log.actions.LogAction;
import org.apache.iceberg.io.log.state.CatalogState;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class CreateTableAction implements LogAction {
  private static final int LATE_BIND = -1;

  private final String name;
  private final int logTblId;
  private final int logTblVersion;
  private final int logNsid;
  private final int logNsVersion;
  private final String location;
  private final Namespace namespace;

  // Namespace created in this transaction
  public CreateTableAction(String name, int logNsid, String location, Namespace namespace) {
    this(name, LATE_BIND, 1, logNsid, LATE_BIND, location, namespace);
    Preconditions.checkArgument(logNsid < 0, "Namespace must be late-bound");
  }

  // Namespace exists
  public CreateTableAction(String name, int logNsid, int logNsVersion, String location, Namespace namespace) {
    this(name, LATE_BIND, 1, logNsid, logNsVersion, location, namespace);
  }

  // created from checkpoint
  public CreateTableAction(
      String name,
      int logTblId,
      int logTblVersion,
      int logNsid,
      int logNsVersion,
      String location,
      Namespace namespace) {
    this.name = name;
    this.logTblId = logTblId;
    this.logTblVersion = logTblVersion;
    this.logNsid = logNsid;
    this.logNsVersion = logNsVersion;
    this.location = location;
    this.namespace = namespace;
  }

  @Override
  public boolean verify(CatalogState state) {
    if (logNsVersion < 0) {
      // contained in a namespace created in this transaction
      return true;
    }
    Integer version = state.namespaceVersion(logNsid);
    return version != null && version == logNsVersion;
  }

  @Override
  public void apply(CatalogState.Builder builder) {
    // restore NSID, version from log (checkpoint)
    final int nsid = logNsid < 0 ? builder.getRemapped(logNsid) : logNsid;
    final int tblId;
    if (this.logTblId == LATE_BIND) {
      tblId = builder.build().nextTblid();
      builder.incrementNextTblid();
    } else {
      tblId = this.logTblId;
    }

    // Get the namespace from the builder's current state
    Namespace resolvedNamespace = builder.build().namespaceRegistry().namespaceById(nsid);
    if (resolvedNamespace == null) {
      // If namespace lookup by ID fails, try to use the stored namespace
      // This can happen during deserialization when remapping information is lost
      if (namespace != null) {
        resolvedNamespace = namespace;
      } else {
        throw new IllegalStateException("Namespace not found for nsid: " + nsid);
      }
    }
    TableIdentifier tableId = TableIdentifier.of(resolvedNamespace, name);

    builder.tableBuilder().addTable(tblId, tableId, logTblVersion, location);
  }

  @Override
  public void serialize(DataOutputStream dos) throws IOException {
    dos.writeByte(Type.CREATE_TABLE.opcode);
    dos.writeUTF(name);
    dos.writeInt(logTblId);
    dos.writeInt(logTblVersion);
    dos.writeInt(logNsid);
    dos.writeInt(logNsVersion);
    dos.writeUTF(location);
    // Serialize namespace
    dos.writeInt(namespace.levels().length);
    for (String level : namespace.levels()) {
      dos.writeUTF(level);
    }
  }

  @Override
  public Type type() {
    return Type.CREATE_TABLE;
  }

  public static CreateTableAction deserialize(DataInputStream dis) throws IOException {
    String name = dis.readUTF();
    int tblId = dis.readInt();
    int tblVersion = dis.readInt();
    int nsid = dis.readInt();
    int nsVersion = dis.readInt();
    String location = dis.readUTF();
    // Deserialize namespace
    int numLevels = dis.readInt();
    String[] levels = new String[numLevels];
    for (int i = 0; i < numLevels; i++) {
      levels[i] = dis.readUTF();
    }
    Namespace namespace = Namespace.of(levels);
    return new CreateTableAction(name, tblId, tblVersion, nsid, nsVersion, location, namespace);
  }

  public String name() {
    return name;
  }

  public int logTblId() {
    return logTblId;
  }

  public int logTblVersion() {
    return logTblVersion;
  }

  public int logNsid() {
    return logNsid;
  }

  public int logNsVersion() {
    return logNsVersion;
  }

  public String location() {
    return location;
  }
}
