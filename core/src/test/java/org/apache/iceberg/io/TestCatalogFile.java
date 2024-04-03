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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Test;

public class TestCatalogFile {

  @Test
  public void testCatalogFileDirectSerialization() throws IOException {
    TableIdentifier tbl1 =
        TableIdentifier.of(Namespace.of("db", "dingos", "yaks", "prod"), "table1");
    TableIdentifier tbl2 =
        TableIdentifier.of(Namespace.of("db", "dingos", "yaks", "prod"), "table2");
    ByteArrayOutputStream ser = new ByteArrayOutputStream();
    CatalogFile catalogFile =
        CatalogFile.empty()
            .addNamespace(Namespace.of("db", "dingos", "yaks", "prod"), Collections.emptyMap())
            .addNamespace(Namespace.of("db", "dingos", "yaks", "qa"), Collections.emptyMap())
            .createTable(tbl1, "gs://bucket/path/to/table1")
            .createTable(tbl2, "gs://bucket/path/to/table2")
            .commit(ser);
    catalogFile.write(ser);
    ByteArrayInputStream deser = new ByteArrayInputStream(ser.toByteArray());
    CatalogFile deserCatalogFile = CatalogFile.read(deser);
    assertThat(deserCatalogFile).isEqualTo(catalogFile);
  }
}
