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

public class DropNamespacePropertyAction implements LogAction {
  private final int nsid;
  private final int nsVersion;
  private final String key;

  public DropNamespacePropertyAction(int nsid, int nsVersion, String key) {
    this.nsid = nsid;
    this.nsVersion = nsVersion;
    this.key = key;
  }

  @Override
  public boolean verify(CatalogState state) {
    Integer currentVersion = state.namespaceVersion(nsid);
    if (currentVersion == null) {
      return false;
    }
    return currentVersion.equals(nsVersion);
  }

  @Override
  public void apply(CatalogState.Builder builder) {
    builder.namespaceBuilder().removeProperty(nsid, key);
    builder.namespaceBuilder().updateVersion(nsid, nsVersion + 1);
  }

  @Override
  public void serialize(DataOutputStream dos) throws IOException {
    dos.writeByte(Type.DROP_NAMESPACE_PROPERTY.ordinal());
    dos.writeInt(nsid);
    dos.writeInt(nsVersion);
    dos.writeUTF(key);
  }

  @Override
  public Type type() {
    return Type.DROP_NAMESPACE_PROPERTY;
  }

  public static DropNamespacePropertyAction deserialize(DataInputStream dis) throws IOException {
    int nsid = dis.readInt();
    int nsVersion = dis.readInt();
    String key = dis.readUTF();
    return new DropNamespacePropertyAction(nsid, nsVersion, key);
  }

  public int nsid() {
    return nsid;
  }

  public int nsVersion() {
    return nsVersion;
  }

  public String key() {
    return key;
  }
}
