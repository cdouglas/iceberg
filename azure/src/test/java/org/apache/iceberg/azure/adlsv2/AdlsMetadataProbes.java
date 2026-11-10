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

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.models.AppendBlobRequestConditions;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.options.AppendBlobCreateOptions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.FileRange;
import com.azure.storage.file.datalake.models.FileReadHeaders;
import com.azure.storage.file.datalake.models.PathProperties;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Probes, on a real ADLS account, of what an object's incarnation kept in its metadata would rest
 * on (fileio-catalog docs/TAIL_READS.md, "Alternatives considered" and step 0b), through the
 * requests the fork's ADLS writes send: a replace stages an Append Blob with the incarnation and
 * renames it onto the target, so the rename must carry the metadata; an AppendBlock must keep it;
 * and a ranged read must report it for the version that served it, while a read pinned to a version
 * a write superseded serves nothing. Set AZURE_STORAGE_ACCOUNT, AZURE_TEST_CONTAINER and
 * AZURE_STORAGE_SAS_TOKEN; skipped otherwise.
 */
public class AdlsMetadataProbes {
  private static final String INCARNATION = "incarnation";

  private static ADLSFileIO io;
  private static String root;
  private static final List<DataLakeFileClient> FILES = Lists.newArrayList();

  @BeforeAll
  static void initStorage() {
    String account = System.getenv("AZURE_STORAGE_ACCOUNT");
    String container = System.getenv("AZURE_TEST_CONTAINER");
    String sasToken = System.getenv("AZURE_STORAGE_SAS_TOKEN");
    if (account != null && container != null && sasToken != null) {
      Map<String, String> properties = Maps.newHashMap();
      properties.put(
          AzureProperties.ADLS_SAS_TOKEN_PREFIX + account + ".dfs.core.windows.net", sasToken);
      io = new ADLSFileIO(properties);
      root =
          "abfs://"
              + container
              + "@"
              + account
              + ".dfs.core.windows.net/metadata-probes/"
              + UUID.randomUUID();
    }
  }

  @AfterAll
  static void cleanUp() {
    for (DataLakeFileClient file : FILES) {
      try {
        file.deleteIfExists();
      } catch (RuntimeException e) {
        // best effort
      }
    }
  }

  @BeforeEach
  void requireCredentials() {
    Assumptions.assumeTrue(io != null, "ADLS not available");
  }

  private static DataLakeFileClient file(String name) {
    String location = root + "/" + name;
    DataLakeFileClient file = io.client(location).getFileClient(new ADLSLocation(location).path());
    FILES.add(file);
    return file;
  }

  private static byte[] bytes(String content) {
    return content.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * A replace as the fork's CAS sends it: an Append Blob staged beside the target, here with the
   * incarnation in its metadata, renamed onto the target if the target is {@code ifMatch} (absent
   * if null). Returns the ETag the rename reports.
   */
  private static String replace(
      DataLakeFileClient target, byte[] content, String incarnation, String ifMatch) {
    DataLakeFileClient temp = file("." + UUID.randomUUID() + ".tmp");
    AppendBlobClient staged = ADLSOutputFile.appendBlob(temp);
    String etag =
        staged
            .createWithResponse(
                new AppendBlobCreateOptions()
                    .setMetadata(Map.of(INCARNATION, incarnation))
                    .setRequestConditions(new BlobRequestConditions().setIfNoneMatch("*")),
                null,
                Context.NONE)
            .getValue()
            .getETag();
    etag =
        staged
            .appendBlockWithResponse(
                new ByteArrayInputStream(content),
                content.length,
                null,
                new AppendBlobRequestConditions().setAppendPosition(0L).setIfMatch(etag),
                null,
                Context.NONE)
            .getValue()
            .getETag();
    DataLakeRequestConditions destination =
        ifMatch == null
            ? new DataLakeRequestConditions().setIfNoneMatch("*")
            : new DataLakeRequestConditions().setIfMatch(ifMatch);
    Response<DataLakeFileClient> renamed =
        temp.renameWithResponse(
            null,
            target.getFilePath(),
            new DataLakeRequestConditions().setIfMatch(etag),
            destination,
            null,
            Context.NONE);
    String reported = renamed.getHeaders().getValue(HttpHeaderName.ETAG);
    return reported != null ? reported : etag;
  }

  /** An append at {@code offset} to the version {@code ifMatch}, as the fork's APPEND sends it. */
  private static String append(
      DataLakeFileClient target, byte[] content, long offset, String ifMatch) {
    return ADLSOutputFile.appendBlob(target)
        .appendBlockWithResponse(
            new ByteArrayInputStream(content),
            content.length,
            null,
            new AppendBlobRequestConditions().setAppendPosition(offset).setIfMatch(ifMatch),
            null,
            Context.NONE)
        .getValue()
        .getETag();
  }

  /** A read from {@code offset} to the end, of the version {@code ifMatch} if not null. */
  private static FileReadHeaders read(
      DataLakeFileClient target, long offset, String ifMatch, ByteArrayOutputStream bytes) {
    DataLakeRequestConditions conditions =
        ifMatch == null ? null : new DataLakeRequestConditions().setIfMatch(ifMatch);
    return target
        .readWithResponse(bytes, new FileRange(offset), null, conditions, false, null, Context.NONE)
        .getDeserializedHeaders();
  }

  private static int status(Runnable request) {
    try {
      request.run();
      return 200;
    } catch (DataLakeStorageException e) {
      return e.getStatusCode();
    } catch (com.azure.storage.blob.models.BlobStorageException e) {
      return e.getStatusCode();
    }
  }

  @Test
  void aReplaceSetsTheIncarnationWithItsBytes() {
    DataLakeFileClient target = file("replace");
    String first = replace(target, bytes("first"), "a", null);
    assertThat(target.getProperties().getMetadata())
        .as("the metadata the rename brought")
        .containsEntry(INCARNATION, "a");

    String second = replace(target, bytes("second"), "b", first);
    PathProperties properties = target.getProperties();
    assertThat(properties.getETag()).isEqualTo(second);
    assertThat(properties.getMetadata()).containsEntry(INCARNATION, "b");
    ByteArrayOutputStream content = new ByteArrayOutputStream();
    assertThat(read(target, 0, null, content).getMetadata()).containsEntry(INCARNATION, "b");
    assertThat(content.toString(StandardCharsets.UTF_8)).isEqualTo("second");
  }

  @Test
  void anAppendKeepsTheIncarnation() {
    DataLakeFileClient target = file("append");
    String created = replace(target, bytes("head"), "a", null);
    String appended = append(target, bytes("-tail"), 4, created);

    PathProperties properties = target.getProperties();
    assertThat(properties.getETag()).isEqualTo(appended).isNotEqualTo(created);
    assertThat(properties.getFileSize()).isEqualTo(9L);
    assertThat(properties.getMetadata())
        .as("the metadata after an append")
        .containsEntry(INCARNATION, "a");
  }

  /**
   * A ranged read reports the incarnation, the version and the object's length of the version that
   * served it, in its response: a tail read from N would be one request. A read pinned to a version
   * an append or a replace superseded serves nothing.
   */
  @Test
  void aRangedReadReportsTheIncarnationOfTheVersionThatServedIt() {
    DataLakeFileClient target = file("ranged");
    String created = replace(target, bytes("0123456789"), "a", null);
    String appended = append(target, bytes("abcdef"), 10, created);

    ByteArrayOutputStream tail = new ByteArrayOutputStream();
    FileReadHeaders headers = read(target, 6, null, tail);
    assertThat(tail.toString(StandardCharsets.UTF_8)).isEqualTo("6789abcdef");
    assertThat(headers.getETag()).isEqualTo(appended);
    assertThat(headers.getContentRange()).isEqualTo("bytes 6-15/16");
    assertThat(headers.getMetadata()).containsEntry(INCARNATION, "a");
    assertThat(status(() -> read(target, 6, created, new ByteArrayOutputStream())))
        .as("a read pinned to the version before an append")
        .isEqualTo(412);

    String replaced = replace(target, bytes("0123456789abcdefXYZ"), "b", appended);
    assertThat(status(() -> read(target, 6, appended, new ByteArrayOutputStream())))
        .as("a read pinned to the version before a replace")
        .isEqualTo(412);
    ByteArrayOutputStream after = new ByteArrayOutputStream();
    assertThat(read(target, 6, replaced, after).getMetadata()).containsEntry(INCARNATION, "b");
    assertThat(after.toString(StandardCharsets.UTF_8)).isEqualTo("6789abcdefXYZ");
  }
}
