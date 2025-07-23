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
package org.apache.iceberg.io.log.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.junit.jupiter.api.Test;

public class TestNamespaceRegistry {

  @Test
  public void testWithRoot() {
    NamespaceRegistry registry = NamespaceRegistry.withRoot();

    assertTrue(registry.contains(Namespace.empty()));
    assertEquals(Integer.valueOf(0), registry.namespaceId(Namespace.empty()));
    assertEquals(Namespace.empty(), registry.namespaceById(0));
    assertEquals(Integer.valueOf(1), registry.version(0));
    assertThat(registry.properties(Namespace.empty())).isEmpty();
  }

  @Test
  public void testBuilder() {
    NamespaceRegistry.Builder builder = NamespaceRegistry.builder();

    // Add a namespace
    builder.addNamespace("test", 0, 1, 1);

    // Add properties
    builder.addProperty(1, "key1", "value1");
    builder.addProperty(1, "key2", "value2");

    NamespaceRegistry registry = builder.build();

    Namespace testNs = Namespace.of("test");
    assertTrue(registry.contains(testNs));
    assertEquals(Integer.valueOf(1), registry.namespaceId(testNs));
    assertEquals(testNs, registry.namespaceById(1));

    Map<String, String> properties = registry.properties(testNs);
    assertEquals("value1", properties.get("key1"));
    assertEquals("value2", properties.get("key2"));
  }

  @Test
  public void testNestedNamespaces() {
    NamespaceRegistry.Builder builder = NamespaceRegistry.builder();

    // Add parent namespace
    builder.addNamespace("parent", 0, 1, 1);
    // Add child namespace
    builder.addNamespace("child", 1, 2, 1);

    NamespaceRegistry registry = builder.build();

    Namespace parentNs = Namespace.of("parent");
    Namespace childNs = Namespace.of("parent", "child");

    assertTrue(registry.contains(parentNs));
    assertTrue(registry.contains(childNs));
    assertEquals(Integer.valueOf(1), registry.namespaceId(parentNs));
    assertEquals(Integer.valueOf(2), registry.namespaceId(childNs));
  }

  @Test
  public void testRemoveNamespace() {
    NamespaceRegistry.Builder builder = NamespaceRegistry.builder();
    builder.addNamespace("test", 0, 1, 1);

    NamespaceRegistry original = builder.build();
    assertTrue(original.contains(Namespace.of("test")));

    NamespaceRegistry.Builder removeBuilder = original.toBuilder();
    removeBuilder.removeNamespace(1);

    NamespaceRegistry updated = removeBuilder.build();
    assertFalse(updated.contains(Namespace.of("test")));
    assertNull(updated.namespaceById(1));
  }

  @Test
  public void testInvalidOperations() {
    NamespaceRegistry.Builder builder = NamespaceRegistry.builder();

    // Test adding property to non-existent namespace
    assertThrows(IllegalArgumentException.class, () -> builder.addProperty(999, "key", "value"));

    // Test removing property from non-existent namespace
    assertThrows(IllegalArgumentException.class, () -> builder.removeProperty(999, "key"));

    // Test removing non-existent namespace
    assertThrows(IllegalStateException.class, () -> builder.removeNamespace(999));
  }

  @Test
  public void testDuplicateNamespace() {
    NamespaceRegistry.Builder builder = NamespaceRegistry.builder();
    builder.addNamespace("test", 0, 1, 1);

    // Try to add the same namespace again with different ID
    assertThrows(IllegalStateException.class, () -> builder.addNamespace("test", 0, 2, 1));
  }
}
