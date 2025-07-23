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

public class DropNamespaceAction implements LogAction {
  private final int nsid;
  private final int version;

  public DropNamespaceAction(int nsid, int version) {
    this.nsid = nsid;
    this.version = version;
  }

  @Override
  public boolean verify(CatalogState state) {
    Integer version = state.namespaceVersion(nsid);
    return version != null && version == this.version;
  }

  @Override
  public void apply(CatalogState.Builder builder) {
    builder.namespaceBuilder().removeNamespace(nsid);
  }

  @Override
  public void serialize(DataOutputStream dos) throws IOException {
    dos.writeByte(Type.DROP_NAMESPACE.opcode);
    dos.writeInt(nsid);
    dos.writeInt(version);
  }

  @Override
  public Type type() {
    return Type.DROP_NAMESPACE;
  }

  public static DropNamespaceAction deserialize(DataInputStream dis) throws IOException {
    int nsid = dis.readInt();
    int version = dis.readInt();
    return new DropNamespaceAction(nsid, version);
  }

  public int nsid() {
    return nsid;
  }

  public int version() {
    return version;
  }
}
