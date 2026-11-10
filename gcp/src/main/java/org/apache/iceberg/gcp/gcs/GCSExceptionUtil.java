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
package org.apache.iceberg.gcp.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageException;
import java.io.IOException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.SupportsAtomicOperations;

final class GCSExceptionUtil {
  private GCSExceptionUtil() {}

  /**
   * A read that asked for {@code generation} fails as stale if the store no longer serves it: a
   * precondition failure (another generation is live), or no such generation, which a read the
   * client library retries after the first response asks for by number.
   */
  static void throwStaleIfGone(IOException ioException, BlobId blobId, Long generation) {
    if (generation != null
        && ioException.getCause() instanceof StorageException storageException
        && (storageException.getCode() == 412 || storageException.getCode() == 404)) {
      throw new SupportsAtomicOperations.StaleReadException(
          "Generation " + generation + " of " + blobId.toGsUtilUri() + " is no longer the object's",
          ioException);
    }
  }

  static void throwNotFoundIfNotPresent(IOException ioException, BlobId blobId) {
    if (ioException.getCause() instanceof StorageException storageException
        && storageException.getCode() == 404) {
      throw new NotFoundException(ioException, "Location does not exist: %s", blobId.toGsUtilUri());
    }
  }
}
