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
import java.io.OutputStream;
import java.util.Collections;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Test;

public class TestCatalogFile {

  private static final Namespace ns1 = Namespace.of("db", "dingos", "yaks", "prod");
  private static final Namespace ns2 = Namespace.of("db", "dingos", "yaks", "qa");
  private static final TableIdentifier tbl1 = TableIdentifier.of(ns1, "table1");
  private static final TableIdentifier tbl2 = TableIdentifier.of(ns1, "table2");
  private static final TableIdentifier tbl3 = TableIdentifier.of(ns1, "table3");
  private static final TableIdentifier tbl4 = TableIdentifier.of(ns1, "table4");

  @Test
  public void testCatalogFileDirectSerialization() {
    ByteArrayOutputStream ser = new ByteArrayOutputStream();
    CatalogFile catalogFile =
        CatalogFile.empty()
            .createNamespace(ns1, Collections.emptyMap())
            .createNamespace(ns2, Collections.emptyMap())
            .createTable(tbl1, "gs://bucket/path/to/table1")
            .createTable(tbl2, "gs://bucket/path/to/table2")
            .commit(ser);
    ByteArrayInputStream deser = new ByteArrayInputStream(ser.toByteArray());
    CatalogFile deserCatalogFile = CatalogFile.read(deser);
    assertThat(deserCatalogFile).isEqualTo(catalogFile);
    assertThat(deserCatalogFile.namespaces())
        .containsExactlyInAnyOrder(Namespace.empty(), ns1, ns2);
    assertThat(deserCatalogFile.tables()).containsExactlyInAnyOrder(tbl1, tbl2);
  }

  @Test
  public void testCatalogBasicDerived() {
    OutputStream nullOut = NullOutputStream.NULL_OUTPUT_STREAM;
    CatalogFile catalogFile =
        CatalogFile.empty()
            .createNamespace(ns1, Collections.emptyMap())
            .createNamespace(ns2, Collections.emptyMap())
            .createTable(tbl1, "gs://bucket/path/to/table1")
            .createTable(tbl2, "gs://bucket/path/to/table2")
            .commit(nullOut);
    ByteArrayOutputStream ser = new ByteArrayOutputStream();
    CatalogFile derived =
        CatalogFile.from(catalogFile)
            .createTable(tbl3, "gs://bucket/path/to/table3")
            .createTable(tbl4, "gs://bucket/path/to/table4")
            .commit(ser);
    ByteArrayInputStream deser = new ByteArrayInputStream(ser.toByteArray());
    CatalogFile deserCatalogFile = CatalogFile.read(deser);
    assertThat(derived).isEqualTo(deserCatalogFile);
    assertThat(deserCatalogFile.namespaces())
        .containsExactlyInAnyOrder(Namespace.empty(), ns1, ns2);
    assertThat(deserCatalogFile.tables()).containsExactlyInAnyOrder(tbl1, tbl2, tbl3, tbl4);
  }
}
