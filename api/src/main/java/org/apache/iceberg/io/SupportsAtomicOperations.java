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

public interface SupportsAtomicOperations<T> extends FileIO {
  /**
   * Create a new atomic output file that will replace the given input file.
   *
   * @param replace an input file to replace
   * @return a new atomic output file
   */
  AtomicOutputFile<T> newOutputFile(InputFile replace);

  class AtomicOperationException extends RuntimeException {
    public AtomicOperationException(String message, Exception cause) {
      super(message, cause);
    }
  }

  class CASException extends AtomicOperationException {
    public CASException(String message, Exception cause) {
      super(message, cause);
    }
  }

  class AppendException extends AtomicOperationException {
    public AppendException(String message, Exception cause) {
      super(message, cause);
    }
  }
}
