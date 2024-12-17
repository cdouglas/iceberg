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

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

public interface AtomicOutputFile<T> extends OutputFile {

  // TODO do this cleanly later; options pattern for prepare, actually align the types
  // TODO so FileIO compatibility is explicit
  // stream(...), checksum(...), length(...), append()
  enum Strategy {
    CAS,
    APPEND,
  }

  /**
   * Generate a token to replace the InputFile with the specified content.
   *
   * @param source Invoked to obtain an InputStream for the future output.
   * @param howto
   */
  T prepare(Supplier<InputStream> source, Strategy howto) throws IOException;

  /**
   * Atomically replace the contents of the target AtomicOutputFile using the contents of the stream
   * provided, only if the content checksum matches.
   *
   * @param token Checksum provided to the underlying store for validation
   * @param source Function invoked to obtain an InputStream to copy to the destination
   * @return an {@link InputFile} with metadata identifying the file written, could be used in a
   *     subsequent call to {@link SupportsAtomicOperations#newOutputFile(InputFile)}
   */
  InputFile writeAtomic(T token, Supplier<InputStream> source) throws IOException;
}
