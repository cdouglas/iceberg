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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

public class TestCatalogFile {

  private static final Namespace NS1 = Namespace.of("db", "dingos", "yaks", "prod");
  private static final Namespace NS2 = Namespace.of("db", "dingos", "yaks", "qa");
  private static final TableIdentifier TBL1 = TableIdentifier.of(NS1, "table1");
  private static final TableIdentifier TBL2 = TableIdentifier.of(NS1, "table2");
  private static final TableIdentifier TBL3 = TableIdentifier.of(NS1, "table3");
  private static final TableIdentifier TBL4 = TableIdentifier.of(NS1, "table4");

  @Test
  public void testCatalogNamespace() {
    OutputStream nullOut = NullOutputStream.NULL_OUTPUT_STREAM;
    CatalogFormat format = new CASCatalogFormat();
    CatalogFile catalogFile =
        format
            .empty()
            .createNamespace(NS1, Collections.emptyMap())
            .createNamespace(NS2, Collections.emptyMap())
            .createTable(TBL1, "gs://bucket/path/to/table1")
            .createTable(TBL2, "gs://bucket/path/to/table2")
            .commit(nullOut);

    final Map<String, String> ns1Props = Collections.singletonMap("key1", "value1");
    CatalogFile updateProp =
        format.from(catalogFile).updateProperties(NS1, ns1Props).commit(nullOut);
    assertThat(updateProp).isNotEqualTo(catalogFile);
    assertThat(updateProp.namespaces()).containsExactlyInAnyOrder(Namespace.empty(), NS1, NS2);
    assertThat(updateProp.namespaceProperties(NS1)).containsExactlyEntriesOf(ns1Props);

    CatalogFile drop = format.from(updateProp).dropNamespace(NS2).commit(nullOut);
    assertThat(drop.namespaces()).containsExactlyInAnyOrder(Namespace.empty(), NS1);
    assertThat(drop.namespaceProperties(NS1)).containsExactlyEntriesOf(ns1Props);
  }

  @Test
  public void testSerDe() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
  }
}
