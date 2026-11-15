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
package org.apache.iceberg.catalog;

import java.util.Map;
import java.util.Set;
import org.immutables.value.Value;

/** A change a catalog transaction made to a namespace. */
@Value.Immutable
public interface NamespaceChange {
  enum Kind {
    /** Create the namespace with {@link #properties()}. */
    CREATE,
    /** Set {@link #properties()} on the namespace. */
    SET_PROPERTIES,
    /** Remove {@link #removals()} from the namespace's properties. */
    REMOVE_PROPERTIES,
    /** Drop the namespace, which must then hold no tables or namespaces. */
    DROP
  }

  Kind kind();

  Namespace namespace();

  Map<String, String> properties();

  Set<String> removals();

  static NamespaceChange create(Namespace namespace, Map<String, String> properties) {
    return ImmutableNamespaceChange.builder()
        .kind(Kind.CREATE)
        .namespace(namespace)
        .properties(properties)
        .build();
  }

  static NamespaceChange setProperties(Namespace namespace, Map<String, String> properties) {
    return ImmutableNamespaceChange.builder()
        .kind(Kind.SET_PROPERTIES)
        .namespace(namespace)
        .properties(properties)
        .build();
  }

  static NamespaceChange removeProperties(Namespace namespace, Set<String> removals) {
    return ImmutableNamespaceChange.builder()
        .kind(Kind.REMOVE_PROPERTIES)
        .namespace(namespace)
        .removals(removals)
        .build();
  }

  static NamespaceChange drop(Namespace namespace) {
    return ImmutableNamespaceChange.builder().kind(Kind.DROP).namespace(namespace).build();
  }
}
