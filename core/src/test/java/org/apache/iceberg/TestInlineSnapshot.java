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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** An {@link InlineSnapshot} whose list a supplier makes on first use. */
public class TestInlineSnapshot {
  private static final ManifestFile DATA = manifest("mem:///m/data.avro", ManifestContent.DATA);
  private static final ManifestFile DELETES =
      manifest("mem:///m/deletes.avro", ManifestContent.DELETES);

  private static ManifestFile manifest(String path, ManifestContent content) {
    return new GenericManifestFile(
        path, 100L, 0, content, 1L, 1L, 7L, null, null, null, null, null, null, null, null, null);
  }

  private static InlineSnapshot snapshot(Supplier<List<ManifestFile>> manifests) {
    return new InlineSnapshot(
        1L, 7L, null, 1000L, DataOperations.APPEND, Map.of(), 0, null, null, null, manifests);
  }

  @Test
  public void aLazyListIsMadeOnceOnFirstUse() {
    AtomicInteger made = new AtomicInteger();
    InlineSnapshot snapshot =
        snapshot(
            () -> {
              made.incrementAndGet();
              return List.of(DATA, DELETES);
            });
    assertThat(snapshot.snapshotId()).isEqualTo(7L);
    assertThat(snapshot.toString()).contains("not yet made");
    assertThat(made).hasValue(0);

    assertThat(snapshot.allManifests(null)).containsExactly(DATA, DELETES);
    assertThat(snapshot.dataManifests(null)).containsExactly(DATA);
    assertThat(snapshot.deleteManifests(null)).containsExactly(DELETES);
    assertThat(made).hasValue(1);
  }

  @Test
  public void aLazySnapshotEqualsTheEagerOne() {
    InlineSnapshot lazy = snapshot(() -> List.of(DATA));
    InlineSnapshot eager =
        new InlineSnapshot(
            1L,
            7L,
            null,
            1000L,
            DataOperations.APPEND,
            Map.of(),
            0,
            null,
            null,
            null,
            List.of(DATA));
    assertThat(lazy).isEqualTo(eager).hasSameHashCodeAs(eager);
  }

  @Test
  public void aSerializedCopyHoldsTheList() throws IOException, ClassNotFoundException {
    // The supplier, a lambda over a local, is not serializable: the copy holds the list it made
    Object unserializable = new Object();
    InlineSnapshot lazy =
        snapshot(
            () -> {
              assertThat(unserializable).isNotNull();
              return List.of(DATA, DELETES);
            });
    InlineSnapshot copy = TestHelpers.roundTripSerialize(lazy);
    assertThat(copy.allManifests(null))
        .extracting(ManifestFile::path)
        .containsExactly(DATA.path(), DELETES.path());
  }
}
