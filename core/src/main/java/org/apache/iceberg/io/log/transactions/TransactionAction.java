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
package org.apache.iceberg.io.log.transactions;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.io.log.actions.LogAction;
import org.apache.iceberg.io.log.serialization.LogActionSerializer;
import org.apache.iceberg.io.log.state.CatalogState;

public class TransactionAction implements LogAction {
  private boolean sealed;
  private final UUID txnId;
  private final List<LogAction> actions;

  public TransactionAction(List<LogAction> actions) {
    this(generateTxnId(), actions, false);
  }

  public TransactionAction(UUID txnId, List<LogAction> actions, boolean sealed) {
    this.txnId = txnId;
    this.actions = actions;
    this.sealed = sealed;
  }

  private static UUID generateTxnId() {
    // Use the same UUID generation as in LogCatalogFile
    long timestamp = System.currentTimeMillis();
    long unixTsMs = timestamp & 0xFFFFFFFFFFFFL; // 48 bits for timestamp

    // Randomness: 12 bits for unique sequencing within the millisecond
    long randA = new java.util.Random().nextInt(0x1000) & 0x0FFF; // 12 bits

    // Construct the most significant 64 bits
    long msb = (unixTsMs << 16) | (0x7L << 12) | randA; // Version 7 (0111)

    // 62 bits of randomness + UUID variant
    long randB = new java.util.Random().nextLong() & 0x3FFFFFFFFFFFFFFFL; // 62 bits
    long lsb = (0x2L << 62) | randB; // Variant bits: 10x (RFC 4122)

    return new UUID(msb, lsb);
  }

  public boolean isSealed() {
    return sealed;
  }

  public void seal() {
    this.sealed = true;
  }

  public UUID txnId() {
    return txnId;
  }

  public List<LogAction> actions() {
    return actions;
  }

  @Override
  public boolean verify(CatalogState state) {
    return actions.stream().allMatch(action -> action.verify(state));
  }

  @Override
  public void apply(CatalogState.Builder builder) {
    actions.forEach(action -> action.apply(builder));
    builder.addCommittedTransaction(txnId);
  }

  @Override
  public void serialize(DataOutputStream dos) throws IOException {
    if (actions.isEmpty()) {
      return;
    }
    dos.writeByte(Type.TRANSACTION.opcode);
    dos.writeLong(txnId.getMostSignificantBits());
    dos.writeLong(txnId.getLeastSignificantBits());
    dos.writeBoolean(sealed);
    dos.writeInt(actions.size());
    for (LogAction action : actions) {
      action.serialize(dos);
    }
  }

  @Override
  public Type type() {
    return Type.TRANSACTION;
  }

  public static void seal(byte[] serTxn) {
    serTxn[17] = 1;
  }

  public static void unseal(byte[] serTxn) {
    serTxn[17] = 0;
  }

  public static TransactionAction deserialize(DataInputStream dis) throws IOException {
    long msb = dis.readLong();
    long lsb = dis.readLong();
    final UUID uuid = new UUID(msb, lsb);
    boolean sealed = dis.readBoolean();
    final int nActions = dis.readInt();
    List<LogAction> actions = new ArrayList<>(nActions);
    for (int i = 0; i < nActions; ++i) {
      actions.add(LogActionSerializer.deserialize(dis));
    }
    return new TransactionAction(uuid, actions, sealed);
  }
}
