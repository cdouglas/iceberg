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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.PathProperties;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.jupiter.api.Test;

class TestADLSInputFile {

  private static final String LOCATION = "abfs://container@account.dfs.core.windows.net/path";

  /**
   * A snapshot pinned to an ETag exists, and knows its length: writing atomically over it needs no
   * existence check.
   */
  @Test
  void aPinnedSnapshotNeedsNoExistenceCheck() {
    DataLakeFileClient fileClient = mock(DataLakeFileClient.class);
    DataLakeFileSystemClient fileSystem = mock(DataLakeFileSystemClient.class);
    when(fileSystem.getFileClient(any())).thenReturn(fileClient);
    ADLSFileIO io = new ADLSFileIO(location -> fileSystem);
    ADLSInputFile snapshot =
        new ADLSInputFile(
            LOCATION,
            12L,
            fileClient,
            mock(),
            MetricsContext.nullMetrics(),
            new DataLakeRequestConditions().setIfMatch("\"pinned\""));

    AtomicOutputFile out = io.newOutputFile(snapshot);

    assertThat(out.toInputFile().getLength()).isEqualTo(12);
    verify(fileClient, never()).exists();
    verify(fileClient, never()).getProperties();
  }

  /**
   * A snapshot that has seen no version assumes the file is absent: writing atomically over it
   * renames with {@code If-None-Match: *}, and no lookup finds that out.
   */
  @Test
  void anUnseenSnapshotCreatesWithoutALookup() {
    DataLakeFileClient fileClient = mock(DataLakeFileClient.class);
    DataLakeFileSystemClient fileSystem = mock(DataLakeFileSystemClient.class);
    when(fileSystem.getFileClient(any())).thenReturn(fileClient);
    ADLSFileIO io = new ADLSFileIO(location -> fileSystem);

    AtomicOutputFile out = io.newOutputFile(io.newInputFile(LOCATION));

    DataLakeRequestConditions conditions = ((ADLSInputFile) out.toInputFile()).conditions();
    assertThat(conditions.getIfNoneMatch()).isEqualTo("*");
    assertThat(conditions.getIfMatch()).isNull();
    verify(fileClient, never()).exists();
    verify(fileClient, never()).getProperties();
  }

  /** An existence check is a lookup the snapshot sees: a CAS from it replaces the version found. */
  @Test
  void existsPinsTheVersionItFinds() {
    DataLakeFileClient fileClient = mock(DataLakeFileClient.class);
    DataLakeFileSystemClient fileSystem = mock(DataLakeFileSystemClient.class);
    when(fileSystem.getFileClient(any())).thenReturn(fileClient);
    PathProperties properties = mock(PathProperties.class);
    when(properties.getETag()).thenReturn("\"seen\"");
    when(properties.getFileSize()).thenReturn(3L);
    when(fileClient.getProperties()).thenReturn(properties);
    ADLSFileIO io = new ADLSFileIO(location -> fileSystem);
    InputFile snapshot = io.newInputFile(LOCATION);

    assertThat(snapshot.exists()).isTrue();
    AtomicOutputFile out = io.newOutputFile(snapshot);

    assertThat(((ADLSInputFile) out.toInputFile()).conditions().getIfMatch()).isEqualTo("\"seen\"");
    assertThat(out.toInputFile().getLength()).isEqualTo(3);
    verify(fileClient, times(1)).getProperties();
    verify(fileClient, never()).exists();
  }
}
