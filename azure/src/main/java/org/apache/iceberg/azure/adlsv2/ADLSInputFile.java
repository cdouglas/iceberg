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
package org.apache.iceberg.azure.adlsv2;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.PathProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;

class ADLSInputFile extends BaseADLSFile implements InputFile {
  private Long fileSize;
  private DataLakeRequestConditions invariants;

  ADLSInputFile(
      String location,
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      MetricsContext metrics) {
    this(location, null, fileClient, azureProperties, metrics, null);
  }

  ADLSInputFile(
      String location,
      Long fileSize,
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      MetricsContext metrics) {
    this(location, fileSize, fileClient, azureProperties, metrics, null);
  }

  ADLSInputFile(
      String location,
      Long fileSize,
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      MetricsContext metrics,
      DataLakeRequestConditions invariants) {
    super(location, fileClient, azureProperties, metrics);
    this.fileSize = fileSize != null && fileSize > 0 ? fileSize : null;
    this.invariants = invariants;
  }

  /**
   * The conditions that select the version of the file this reads, and that a replace of it
   * expects: the version its first request found (a stream's first read, or the properties its
   * length came from), else its current one.
   */
  DataLakeRequestConditions conditions() {
    if (null == invariants) {
      try {
        // Base::pathProperties() caches its response, should be invariant
        pin(pathProperties());
      } catch (DataLakeStorageException e) {
        if (e.getStatusCode() == 404) {
          // *should* be possible to atomically get properties and !exists, but whatever
          invariants = new DataLakeRequestConditions().setIfNoneMatch("*");
        } else {
          throw e;
        }
      }
    }
    return invariants;
  }

  @Override
  public long getLength() {
    if (fileSize == null) {
      PathProperties properties = pathProperties();
      pin(properties);
      if (fileSize == null) {
        this.fileSize = properties.getFileSize();
      }
    }
    return fileSize;
  }

  /**
   * A stream of the version of the file this reads. It sends nothing until it reads; if the file
   * has no version yet, its first request chooses one, with no lookup of its own.
   */
  @Override
  public SeekableInputStream newStream() {
    return new ADLSInputStream(
        location(), fileClient(), fileSize, azureProperties(), this::version, this::pin, metrics());
  }

  private synchronized DataLakeRequestConditions version() {
    return invariants;
  }

  /**
   * Chooses the version {@code properties} describe, and its length, if the file has no version
   * yet: what a request first finds is what its later reads, and a replace, see.
   */
  private synchronized void pin(PathProperties properties) {
    if (invariants == null && properties != null && properties.getETag() != null) {
      invariants = new DataLakeRequestConditions().setIfMatch(properties.getETag());
      if (fileSize == null) {
        this.fileSize = properties.getFileSize();
      }
    }
  }
}
