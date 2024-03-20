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
package org.apache.iceberg.io;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

public interface CatalogFile {
    // TODO tombstone table data? Or allow ABA?
    Types.StructType TABLE =
            Types.StructType.of(
                    Types.NestedField.required(100, "table", Types.StructType.of(
                    Types.NestedField.required(102, "namespace", Types.StringType.get()),
                    Types.NestedField.required(103, "tableName", Types.StringType.get()),
                    Types.NestedField.required(104, "location", Types.StringType.get()),
                    Types.NestedField.optional(105, "metadata", Types.StringType.get())
            )));
    Types.NestedField TABLES =
            Types.NestedField.optional(106,
                "tables",
                Types.ListType.ofRequired(107, TABLE),
                "list of tables");
    Schema SCHEMA = new Schema();

    static Schema schema() {
        return SCHEMA;
    }
}
