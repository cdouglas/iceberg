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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Test;

public class TestCatalogFile {

  @Test
  public void testCatalogFileDirectSerialization() {
    CatalogFile catalogFile =
        new CatalogFile(
            Stream.of(
                    new Object[][] {
                      {
                        new String[] {"db", "dingos", "yaks", "prod"},
                        new CatalogFile.TableInfo(3, "gs://bucket0/chinchillas")
                      },
                      {
                        new String[] {"db", "dingos", "yaks", "qa"},
                        new CatalogFile.TableInfo(3, "gs://bucket1/chinchillas")
                      },
                    })
                .collect(
                    Collectors.toMap(
                        x -> TableIdentifier.of((String[]) x[0]),
                        x -> (CatalogFile.TableInfo) x[1])));
    ByteArrayOutputStream ser = new ByteArrayOutputStream();
    catalogFile.write(ser);
    ByteArrayInputStream deser = new ByteArrayInputStream(ser.toByteArray());
    CatalogFile deserCatalogFile = new CatalogFile();
    deserCatalogFile.read(deser);
    assert catalogFile.equals(deserCatalogFile);
  }
}
