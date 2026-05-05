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

public interface SupportsAtomicOperations extends FileIO {
  /**
   * Create a new atomic output file that will replace the given input file.
   *
   * @param replace an input file to replace
   * @return a new atomic output file
   */
  AtomicOutputFile newOutputFile(InputFile replace);

  /**
   * Whether this FileIO supports the {@link AtomicOutputFile.Strategy#APPEND} strategy on top of an
   * existing object. Backends that have no append primitive (e.g. GCS objects are immutable --
   * every write replaces the whole object) return {@code false}; callers that mix CAS and APPEND in
   * a commit log must fall back to CAS-only on those backends. Defaults to {@code true} since most
   * backends with conditional writes can also support an offset-pinned append.
   */
  default boolean supportsAppend() {
    return true;
  }

  class AtomicOperationException extends RuntimeException {
    public AtomicOperationException(String message, Exception cause) {
      super(message, cause);
    }
  }

  /**
   * Thrown when an atomic CAS write cannot complete. Subclasses distinguish a real precondition
   * failure (storage rejected the write because our snapshot was stale) from a transient
   * backpressure signal where retrying the same write should succeed. Callers that don't care about
   * the distinction can catch this base type.
   */
  class CASException extends AtomicOperationException {
    public CASException(String message, Exception cause) {
      super(message, cause);
    }
  }

  /**
   * Storage rejected the write because its CAS precondition failed: the target's ETag/generation no
   * longer matches the snapshot we pinned, or "object must not exist" lost a create race. Retrying
   * the same write will fail again -- the caller must re-read state and reconcile before another
   * attempt.
   */
  class StorageInvariantException extends CASException {
    public StorageInvariantException(String message, Exception cause) {
      super(message, cause);
    }
  }

  /**
   * Storage applied backpressure (HTTP 429 rate limit, 5xx transient server error, GCS per-object
   * update rate). Underlying invariants still hold; the caller should retry the same write after a
   * backoff.
   */
  class StorageThrottleException extends CASException {
    public StorageThrottleException(String message, Exception cause) {
      super(message, cause);
    }
  }

  class AppendException extends AtomicOperationException {
    public AppendException(String message, Exception cause) {
      super(message, cause);
    }
  }
}
