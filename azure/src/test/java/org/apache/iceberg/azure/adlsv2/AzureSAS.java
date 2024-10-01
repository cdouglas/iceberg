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
import com.azure.storage.file.datalake.DataLakePathClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * surely there's a format for this already. Whatever.
 *
 * <p>{ "endpoint": "https://lstnsgym.blob.core.windows.net", "sasToken": "", "connectionString":
 * "", "account": "lstnsgym", "container": "lst-ns-consistency" }
 */
class AzureSAS {
  String endpoint;
  String sasToken;
  String connectionString;
  String account;
  String container;

  public void setAccount(String account) {
    this.account = account;
  }

  public void setContainer(String container) {
    this.container = container;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public void setSasToken(String sasToken) {
    this.sasToken = sasToken;
  }

  public void setConnectionString(String connectionString) {
    this.connectionString = connectionString;
  }

  static AzureSAS readCreds(File json) {
    ObjectMapper objMapper = new ObjectMapper();
    try (FileInputStream in = new FileInputStream(json)) {
      return objMapper.readValue(in, AzureSAS.class);
    } catch (FileNotFoundException e) {
      return null;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static class SasResolver implements LocationResolver {

    private final String sasToken;
    private final String endpoint;
    private final String connStr;
    private final String account;
    private final String container;

    SasResolver(AzureSAS fromJson) {
      this.endpoint = fromJson.endpoint;
      this.sasToken = fromJson.sasToken;
      this.connStr = fromJson.connectionString;
      this.account = fromJson.account;
      this.container = fromJson.container;
    }

    @Override
    public String endpoint() {
      return endpoint;
    }

    @Override
    public DataLakeFileClient fileClient(String path) {
      return new DataLakePathClientBuilder()
          .endpoint(endpoint())
          .sasToken(sasToken)
          .fileSystemName(container())
          .pathName(path)
          .buildFileClient();
    }

    @Override
    public DataLakeServiceClient serviceClient() {
      return new DataLakeServiceClientBuilder()
          .endpoint(endpoint())
          .sasToken(sasToken)
          .buildClient();
    }

    @Override
    public String account() {
      return account;
    }

    @Override
    public String container() {
      return container;
    }
  }
}
