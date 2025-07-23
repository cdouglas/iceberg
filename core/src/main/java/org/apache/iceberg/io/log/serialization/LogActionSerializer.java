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
package org.apache.iceberg.io.log.serialization;

import java.io.DataInputStream;
import java.io.IOException;
import org.apache.iceberg.io.log.actions.CheckpointAction;
import org.apache.iceberg.io.log.actions.LogAction;
import org.apache.iceberg.io.log.actions.namespace.AddNamespacePropertyAction;
import org.apache.iceberg.io.log.actions.namespace.CreateNamespaceAction;
import org.apache.iceberg.io.log.actions.namespace.DropNamespaceAction;
import org.apache.iceberg.io.log.actions.namespace.DropNamespacePropertyAction;
import org.apache.iceberg.io.log.actions.table.CreateTableAction;
import org.apache.iceberg.io.log.actions.table.DropTableAction;
import org.apache.iceberg.io.log.actions.table.ReadTableAction;
import org.apache.iceberg.io.log.actions.table.UpdateTableAction;
import org.apache.iceberg.io.log.transactions.TransactionAction;

public class LogActionSerializer {

  public static LogAction deserialize(DataInputStream dis) throws IOException {
    LogAction.Type type = LogAction.Type.from(dis.readByte());

    switch (type) {
      case CHECKPOINT:
        return CheckpointAction.deserialize(dis);
      case CREATE_NAMESPACE:
        return CreateNamespaceAction.deserialize(dis);
      case DROP_NAMESPACE:
        return DropNamespaceAction.deserialize(dis);
      case CREATE_TABLE:
        return CreateTableAction.deserialize(dis);
      case UPDATE_TABLE:
        return UpdateTableAction.deserialize(dis);
      case READ_TABLE:
        return ReadTableAction.deserialize(dis);
      case DROP_TABLE:
        return DropTableAction.deserialize(dis);
      case ADD_NAMESPACE_PROPERTY:
        return AddNamespacePropertyAction.deserialize(dis);
      case DROP_NAMESPACE_PROPERTY:
        return DropNamespacePropertyAction.deserialize(dis);
      case TRANSACTION:
        return TransactionAction.deserialize(dis);
      default:
        throw new IllegalArgumentException("Unknown action type: " + type);
    }
  }
}
