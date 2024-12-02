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

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.models.PathInfo;
import com.azure.storage.file.datalake.options.FileParallelUploadOptions;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.FileChecksum;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.metrics.MetricsContext;

class ADLSOutputFile extends BaseADLSFile implements AtomicOutputFile {

  private final DataLakeRequestConditions conditions;

  ADLSOutputFile(
      String location,
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      MetricsContext metrics) {
    this(location, fileClient, azureProperties, null, metrics);
  }

  ADLSOutputFile(
      String location,
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      DataLakeRequestConditions conditions,
      MetricsContext metrics) {
    super(location, fileClient, azureProperties, metrics);
    this.conditions = conditions;
  }

  /**
   * Create an output stream for the specified location if the target object does not exist in Azure
   * at the time of invocation.
   *
   * @return output stream
   */
  @Override
  public PositionOutputStream create() {
    if (!exists()) {
      return createOrOverwrite();
    } else {
      throw new AlreadyExistsException("Location already exists: %s", location());
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    return new ADLSOutputStream(fileClient(), azureProperties(), metrics());
  }

  @Override
  public InputFile toInputFile() {
    return new ADLSInputFile(location(), fileClient(), azureProperties(), metrics());
  }

  @Override
  public FileChecksum checksum() {
    return new ADLSChecksum();
  }

  @Override
  public ADLSInputFile writeAtomic(FileChecksum checksum, Supplier<InputStream> source)
      throws IOException {
    // Annoyingly, the checksum is not validated server-side, but stored as metadata. The
    // partial-write
    // problem is less of an issue using an InputStream, so the length validation can suffice
    try (InputStream src = source.get()) {
      // TODO local runner doesn't support this API, testcontainer path will fail
      final Response<PathInfo> resp =
          fileClient()
              .uploadWithResponse(
                  new FileParallelUploadOptions(src, checksum.contentLength())
                      .setRequestConditions(conditions)
                      .setHeaders(
                          new PathHttpHeaders()
                              .setContentMd5(checksum.asBytes())
                              .setContentType("binary")),
                  null, // no timeout
                  Context.NONE);
      final PathInfo info = resp.getValue();
      return new ADLSInputFile(
          location(),
          checksum.contentLength(),
          fileClient(),
          azureProperties(),
          metrics(),
          new DataLakeRequestConditions().setIfMatch(info.getETag()));
    } catch (DataLakeStorageException e) {
      if (412 == e.getStatusCode()) {
        // precondition failed
        throw new SupportsAtomicOperations.CASException("Target modified", e);
      }
      throw e;
    }
  }
}
