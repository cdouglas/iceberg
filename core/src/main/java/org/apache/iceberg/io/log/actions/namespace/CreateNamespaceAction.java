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
package org.apache.iceberg.io.log.actions.namespace;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.iceberg.io.log.actions.LogAction;
import org.apache.iceberg.io.log.state.CatalogState;

public class CreateNamespaceAction implements LogAction {
  private static final int LATE_BIND = -1;

  private final String name;
  private final int logNsid;
  private final int logVersion;
  private final int logParentId;
  private final int logParentVersion;

  // log, parent created in this transaction
  public CreateNamespaceAction(String name, int logVersion, int logParentId) {
    this(name, logVersion, LATE_BIND, logParentId, LATE_BIND);
  }

  // log, parent exists
  public CreateNamespaceAction(String name, int logNsid, int logParentId, int logParentVersion) {
    this(name, logNsid, LATE_BIND, logParentId, logParentVersion);
  }

  // checkpoint
  public CreateNamespaceAction(
      String name, int logNsid, int logVersion, int logParentId, int logParentVersion) {
    this.name = name;
    this.logNsid = logNsid;
    this.logVersion = logVersion;
    this.logParentId = logParentId;
    this.logParentVersion = logParentVersion;
  }

  @Override
  public boolean verify(CatalogState state) {
    // concurrent creates are conflicts, but can be retried
    if (logParentVersion < 0) {
      return true;
    }
    Integer version = state.namespaceVersion(logParentId);
    return version != null && version == logParentVersion;
  }

  @Override
  public void apply(CatalogState.Builder builder) {
    // increment parent version, assign uniq nsid
    final int nsid, parentId, version;
    if (logNsid < 0) {
      // assign late-bound NSID, record remap
      nsid = builder.remap(logNsid);
      parentId = logParentId < 0 ? builder.getRemapped(logParentId) : logParentId;
      version = 1;
      builder
          .namespaceBuilder()
          .updateVersion(parentId, builder.namespaceBuilder().getVersion(parentId) + 1);
    } else {
      // restore NSID, version from log (checkpoint)
      nsid = logNsid;
      parentId = logParentId;
      version = logVersion;
    }
    builder.namespaceBuilder().addNamespace(name, parentId, nsid, version);
  }

  @Override
  public void serialize(DataOutputStream dos) throws IOException {
    dos.writeByte(Type.CREATE_NAMESPACE.opcode);
    dos.writeUTF(name);
    dos.writeInt(logNsid);
    dos.writeInt(logVersion);
    dos.writeInt(logParentId);
    dos.writeInt(logParentVersion);
  }

  @Override
  public Type type() {
    return Type.CREATE_NAMESPACE;
  }

  public static CreateNamespaceAction deserialize(DataInputStream dis) throws IOException {
    String name = dis.readUTF();
    int nsid = dis.readInt();
    int version = dis.readInt();
    int parentId = dis.readInt();
    int parentVersion = dis.readInt();
    return new CreateNamespaceAction(name, nsid, version, parentId, parentVersion);
  }

  public String name() {
    return name;
  }

  public int logNsid() {
    return logNsid;
  }

  public int logVersion() {
    return logVersion;
  }

  public int logParentId() {
    return logParentId;
  }

  public int logParentVersion() {
    return logParentVersion;
  }
}
