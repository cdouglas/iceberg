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
   * Create a new atomic output file that will replace the given input file: the version of the
   * object it saw.
   *
   * <p>A snapshot has seen a version if a write returned it, or if a lookup through it ({@code
   * getLength()}, {@code exists()}) or, on a store whose reads record it, a read found one. A CAS
   * replaces only that version. A snapshot that has seen none, such as a fresh {@link
   * #newInputFile(String)}, assumes the object does not exist, as {@link OutputFile#create()} does
   * for a new file: a CAS from it creates the object only if none exists, and sends no lookup to
   * learn that. To replace an object it has not read, a caller looks it up first. An APPEND extends
   * an existing object, so an APPEND from a snapshot that has seen no version looks the object up
   * when it writes.
   *
   * @param replace a snapshot of the object to replace
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

  /**
   * The most bytes one {@link AtomicOutputFile.Strategy#APPEND} to the object at {@code location}
   * may write, or 0 if the store sets no limit an append could reach. A larger append fails without
   * landing (Azure caps an {@code AppendBlock}: 4 MiB before service version 2022-11-02, 100 MiB
   * since); a CAS is not bound by it. Defaults to 0.
   */
  default long maxAppendBytes(String location) {
    return 0L;
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
   *
   * <p>This is no proof that the write did not land: when an SDK retries a write whose reply was
   * lost, the retry fails its precondition on the version the first attempt wrote. That write
   * landed before the reply, so a read that does not show it rules it out.
   */
  class StorageInvariantException extends CASException {
    public StorageInvariantException(String message, Exception cause) {
      super(message, cause);
    }
  }

  /**
   * Storage applied backpressure (HTTP 429 rate limit, 5xx transient server error, GCS per-object
   * update rate). The caller may retry the same write after a backoff: its precondition still
   * guards it.
   *
   * <p>The write may have landed nonetheless. A 5xx can follow a write the service applied, and an
   * SDK that retries internally reports only its last attempt, which a write that landed on an
   * earlier one does not prevent. A retry of a write that landed fails its precondition; if the
   * caller stops retrying instead, it must read the target to learn whether the write took effect.
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

  /**
   * Storage rejected an APPEND because the object holds as many writes as the store allows: S3
   * Express caps an object at 10,000 parts, and Azure an Append Blob at 50,000 blocks. Nothing
   * landed, and no append to this object can succeed; the caller must replace the object (CAS) to
   * append again. Not an {@link AppendException}, which a caller retries as an append at the
   * object's new length.
   */
  class SegmentLimitException extends AtomicOperationException {
    public SegmentLimitException(String message, Exception cause) {
      super(message, cause);
    }
  }
}
