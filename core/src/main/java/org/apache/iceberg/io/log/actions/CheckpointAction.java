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
package org.apache.iceberg.io.log.actions;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;
import org.apache.iceberg.io.log.state.CatalogState;

public class CheckpointAction implements LogAction {
  private final UUID catalogUUID;
  private final int nextNsid;
  private final int nextTblid;
  private final int chkLen;
  private final int tblEmbedEnd;
  private final int committedTxnLen;
  private final int rootNamespaceVersion;

  // Backwards compatibility constructor
  public CheckpointAction(
      UUID catalogUUID,
      int nextNsid,
      int nextTblid,
      int chkLen,
      int tblEmbedEnd,
      int committedTxnLen) {
    this(catalogUUID, nextNsid, nextTblid, chkLen, tblEmbedEnd, committedTxnLen, 1);
  }

  public CheckpointAction(
      UUID catalogUUID,
      int nextNsid,
      int nextTblid,
      int chkLen,
      int tblEmbedEnd,
      int committedTxnLen,
      int rootNamespaceVersion) {
    this.catalogUUID = catalogUUID;
    this.nextNsid = nextNsid;
    this.nextTblid = nextTblid;
    this.chkLen = chkLen;
    this.tblEmbedEnd = tblEmbedEnd;
    this.committedTxnLen = committedTxnLen;
    this.rootNamespaceVersion = rootNamespaceVersion;
  }

  public int length() {
    // opcode UUID [fields]
    return 1 + 16 + 6 * Integer.BYTES;
  }

  @Override
  public boolean verify(CatalogState state) {
    return true;
  }

  @Override
  public void apply(CatalogState.Builder builder) {
    builder.setGlobals(catalogUUID, nextNsid, nextTblid);
    // Set root namespace version if different from default
    if (rootNamespaceVersion != 1) {
      builder.namespaceBuilder().updateVersion(0, rootNamespaceVersion);
    }
  }

  @Override
  public void serialize(DataOutputStream dos) throws IOException {
    dos.writeByte(Type.CHECKPOINT.opcode);
    dos.writeLong(catalogUUID.getMostSignificantBits());
    dos.writeLong(catalogUUID.getLeastSignificantBits());
    dos.writeInt(nextNsid);
    dos.writeInt(nextTblid);
    dos.writeInt(chkLen);
    dos.writeInt(tblEmbedEnd);
    dos.writeInt(committedTxnLen);
    dos.writeInt(rootNamespaceVersion);
  }

  @Override
  public Type type() {
    return Type.CHECKPOINT;
  }

  public static CheckpointAction deserialize(DataInputStream dis) throws IOException {
    long msb = dis.readLong();
    long lsb = dis.readLong();
    UUID catalogUUID = new UUID(msb, lsb);
    int nextNsid = dis.readInt();
    int nextTblid = dis.readInt();
    int chkLen = dis.readInt();
    int tblEmbedEnd = dis.readInt();
    int committedTxnLen = dis.readInt();
    int rootNamespaceVersion = dis.readInt();
    return new CheckpointAction(
        catalogUUID, nextNsid, nextTblid, chkLen, tblEmbedEnd, committedTxnLen, rootNamespaceVersion);
  }

  public UUID catalogUUID() {
    return catalogUUID;
  }

  public int nextNsid() {
    return nextNsid;
  }

  public int nextTblid() {
    return nextTblid;
  }

  public int chkLen() {
    return chkLen;
  }

  public int tblEmbedEnd() {
    return tblEmbedEnd;
  }

  public int committedTxnLen() {
    return committedTxnLen;
  }
}
