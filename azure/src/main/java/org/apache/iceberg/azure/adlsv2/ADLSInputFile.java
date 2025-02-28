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
    this(location, null, fileClient, azureProperties, metrics);
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

  DataLakeRequestConditions conditions() {
    if (null == invariants) {
      try {
        // Base::pathProperties() caches its response, should be invariant
        invariants = new DataLakeRequestConditions().setIfMatch(pathProperties().getETag());
      } catch (DataLakeStorageException e) {
        if (e.getStatusCode() == 404) {
          // *should* be possible to atomically get properties and !exists, but whatever
          return null;
        }
      }
    }
    return invariants;
  }

  @Override
  public long getLength() {
    if (fileSize == null) {
      this.fileSize = pathProperties().getFileSize(); // !#! XXX assumed not null?
    }
    return fileSize;
  }

  @Override
  public SeekableInputStream newStream() {
    ADLSInputStream ret =
        new ADLSInputStream(fileClient(), fileSize, azureProperties(), invariants, metrics());
    if (null == invariants) {
      // !#! convoluted flow trying to avoid an unnecessary metadata lookup if the InputFile is
      // immediately resolved
      invariants = new DataLakeRequestConditions().setIfMatch(ret.pathProperties().getETag());
    }
    return ret;
  }
}
