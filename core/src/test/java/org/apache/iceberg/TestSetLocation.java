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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSetLocation extends TestBase {

  @TestTemplate
  public void setLocation() {
    String location = tableDir.toURI() + "moved";

    table.updateLocation().setLocation(location).commit();

    assertThat(readMetadata().location()).isEqualTo(location);
  }

  @TestTemplate
  public void setLocationRetryWithConcurrentModification() {
    String location = tableDir.toURI() + "moved";

    // On the first commit attempt, another writer modifies the table
    TableOperations concurrentOps =
        new TestTables.TestTableOperations("test", tableDir, table.ops().io()) {
          private boolean firstAttempt = true;

          @Override
          public void commit(TableMetadata base, TableMetadata metadata) {
            if (firstAttempt) {
              firstAttempt = false;
              table.updateProperties().set("key", "value").commit();
            }

            super.commit(base, metadata);
          }
        };

    new SetLocation(concurrentOps).setLocation(location).commit();

    TableMetadata metadata = readMetadata();
    assertThat(metadata.location()).isEqualTo(location);
    assertThat(metadata.properties()).containsEntry("key", "value");
  }
}
