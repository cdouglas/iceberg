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

/** I */
public abstract class CatalogFormat {

  // Ah. you were trying to ensure the CatalogFormat and FileIO class
  // were compatible... not sure this is useful, as generic type parameters
  // are discarded at runtime? Remove generic from type for now, maybe
  // revisit later?
  // would need this parameter to get bound to the CatalogFile<T> s.t.
  // it could not be passed to a FileIO class that did not support it

  public abstract CatalogFile.Mut empty(InputFile inputFile);

  public abstract CatalogFile read(SupportsAtomicOperations fileIO, InputFile in);

  public abstract CatalogFile.Mut from(CatalogFile other);
}
