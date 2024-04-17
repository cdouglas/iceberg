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

import java.util.function.Consumer;
import java.util.zip.Checksum;

public interface AtomicOutputFile extends OutputFile {
  Checksum checksum();

  /**
   * Create a stream that- if the content written matches the checksum- will replace this file.
   * Callback may return the InputFile corresponding to what was written, on close. This API is bad;
   * the semantics are ambiguous. The callback only happens if the file closes. The V2 APIs include
   * a Future-like API, which should admit a cleaner API.
   *
   * @param checksum the checksum to validate the content
   * @param onClose the callback to run on close
   */
  PositionOutputStream createAtomic(Checksum checksum, Consumer<InputFile> onClose);
}
