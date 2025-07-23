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

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.iceberg.io.log.state.CatalogState;

public interface LogAction {
  enum Type {
    CHECKPOINT(0),
    CREATE_TABLE(1),
    UPDATE_TABLE(2),
    READ_TABLE(3),
    DROP_TABLE(4),
    CREATE_NAMESPACE(5),
    DROP_NAMESPACE(6),
    ADD_NAMESPACE_PROPERTY(7),
    DROP_NAMESPACE_PROPERTY(8),
    TRANSACTION(9);

    public final int opcode;

    Type(int opcode) {
      this.opcode = opcode;
    }

    public static Type from(int opcode) {
      for (Type t : Type.values()) {
        if (t.opcode == opcode) {
          return t;
        }
      }
      throw new IllegalArgumentException("Unknown opcode: " + opcode);
    }
  }

  boolean verify(CatalogState state);

  void apply(CatalogState.Builder builder);

  void serialize(DataOutputStream dos) throws IOException;

  Type type();
}
