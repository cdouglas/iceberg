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
package org.apache.iceberg.benchmark.remapping.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for cloud storage FileIO implementations.
 *
 * <p>These tests verify that GCS, S3, and Azure FileIO implementations work correctly for the
 * benchmark workloads (writing and reading Puffin files for deletion vectors).
 *
 * <p>These tests are disabled by default and only run when explicitly invoked with the appropriate
 * environment variables set:
 *
 * <ul>
 *   <li>GCS: Set GCS_TEST_BUCKET (e.g., "gs://my-bucket/test")
 *   <li>S3: Set S3_TEST_BUCKET (e.g., "s3://my-bucket/test") and AWS credentials
 *   <li>Azure: Set AZURE_TEST_URI (e.g., "abfss://container@account.dfs.core.windows.net/test")
 * </ul>
 *
 * <p>Run with: ./gradlew :benchmark:remapping-microbenchmark:test --tests
 * "*CloudStorageIntegrationTest*" -Ptags=integration
 */
@Tag("integration")
public class CloudStorageIntegrationTest {

  private static FileIO fileIO;
  private static final String DV_BLOB_TYPE = "apache-iceberg-dv-v1";

  @BeforeAll
  static void setup() {
    Map<String, String> properties = new HashMap<>();
    fileIO = new ResolvingFileIO();
    fileIO.initialize(properties);
  }

  @AfterAll
  static void cleanup() throws IOException {
    if (fileIO != null) {
      fileIO.close();
    }
  }

  @Test
  void testGcsWriteAndRead() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set to run GCS integration tests").isNotNull();

    String testPath = bucket + "/integration-test-" + UUID.randomUUID() + "/test.puffin";
    try {
      verifyWriteAndRead(testPath);
    } finally {
      cleanupFile(testPath);
    }
  }

  @Test
  void testS3WriteAndRead() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set to run S3 integration tests").isNotNull();

    String testPath = bucket + "/integration-test-" + UUID.randomUUID() + "/test.puffin";
    try {
      verifyWriteAndRead(testPath);
    } finally {
      cleanupFile(testPath);
    }
  }

  @Test
  void testAzureWriteAndRead() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set to run Azure integration tests").isNotNull();

    String testPath = uri + "/integration-test-" + UUID.randomUUID() + "/test.puffin";
    try {
      verifyWriteAndRead(testPath);
    } finally {
      cleanupFile(testPath);
    }
  }

  @Test
  void testGcsFileLengthAfterWrite() throws IOException {
    String bucket = System.getenv("GCS_TEST_BUCKET");
    assumeThat(bucket).as("GCS_TEST_BUCKET must be set to run GCS integration tests").isNotNull();

    String testPath = bucket + "/length-test-" + UUID.randomUUID() + "/test.puffin";
    try {
      verifyFileLengthAfterWrite(testPath);
    } finally {
      cleanupFile(testPath);
    }
  }

  @Test
  void testS3FileLengthAfterWrite() throws IOException {
    String bucket = System.getenv("S3_TEST_BUCKET");
    assumeThat(bucket).as("S3_TEST_BUCKET must be set to run S3 integration tests").isNotNull();

    String testPath = bucket + "/length-test-" + UUID.randomUUID() + "/test.puffin";
    try {
      verifyFileLengthAfterWrite(testPath);
    } finally {
      cleanupFile(testPath);
    }
  }

  @Test
  void testAzureFileLengthAfterWrite() throws IOException {
    String uri = System.getenv("AZURE_TEST_URI");
    assumeThat(uri).as("AZURE_TEST_URI must be set to run Azure integration tests").isNotNull();

    String testPath = uri + "/length-test-" + UUID.randomUUID() + "/test.puffin";
    try {
      verifyFileLengthAfterWrite(testPath);
    } finally {
      cleanupFile(testPath);
    }
  }

  private void verifyWriteAndRead(String path) throws IOException {
    OutputFile outputFile = fileIO.newOutputFile(path);

    // Write a Puffin file with a deletion vector blob
    byte[] testData = "test deletion vector bitmap data".getBytes(StandardCharsets.UTF_8);
    ByteBuffer buffer = ByteBuffer.wrap(testData);

    try (PuffinWriter writer = Puffin.write(outputFile).build()) {
      Blob blob =
          new Blob(
              DV_BLOB_TYPE,
              Collections.singletonList(1),
              0,
              0,
              buffer,
              null,
              Collections.singletonMap("referenced-data-file", "data-00000.parquet"));
      writer.add(blob);
      writer.finish();
    }

    // Read back and verify
    InputFile inputFile = outputFile.toInputFile();
    assertThat(inputFile.exists()).as("File should exist after write").isTrue();

    try (PuffinReader reader = Puffin.read(inputFile).build()) {
      var blobs = reader.fileMetadata().blobs();
      assertThat(blobs).hasSize(1);
      assertThat(blobs.get(0).type()).isEqualTo(DV_BLOB_TYPE);
      assertThat(blobs.get(0).properties())
          .containsEntry("referenced-data-file", "data-00000.parquet");
    }
  }

  private void verifyFileLengthAfterWrite(String path) throws IOException {
    OutputFile outputFile = fileIO.newOutputFile(path);

    // Write test data
    byte[] testData = "test data for length verification".getBytes(StandardCharsets.UTF_8);
    ByteBuffer buffer = ByteBuffer.wrap(testData);

    try (PuffinWriter writer = Puffin.write(outputFile).build()) {
      Blob blob =
          new Blob(
              DV_BLOB_TYPE,
              Collections.singletonList(1),
              0,
              0,
              buffer,
              null,
              Collections.singletonMap("referenced-data-file", "data-00000.parquet"));
      writer.add(blob);
      writer.finish();
    }

    // Immediately try to get file length - this is the operation that was failing
    InputFile inputFile = outputFile.toInputFile();
    long length = inputFile.getLength();

    assertThat(length).as("File length should be positive after write").isGreaterThan(0);

    // The Puffin format adds header/footer overhead
    assertThat(length)
        .as("File length should be at least the size of test data")
        .isGreaterThanOrEqualTo(testData.length);
  }

  private void cleanupFile(String path) {
    try {
      fileIO.deleteFile(path);
    } catch (Exception e) {
      // Ignore cleanup errors
    }
  }
}
