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
import com.azure.storage.file.datalake.DataLakeServiceClient;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;

/** Crude indirection for AzureContainer/actual Azure paths in tests. */
interface LocationResolver {
  default String location(String path) {
    return String.format("abfs://%s@%s.dfs.core.windows.net/%s", container(), account(), path);
  }

  String endpoint();

  DataLakeFileClient fileClient(String path);

  DataLakeServiceClient serviceClient();

  String account();

  String container();

  default void createFile(String path, byte[] data) {
    try (OutputStream out = fileClient(path).getOutputStream()) {
      out.write(data);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
