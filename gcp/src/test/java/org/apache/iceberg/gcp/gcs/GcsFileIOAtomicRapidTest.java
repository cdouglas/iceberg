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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.io.SupportsAtomicOperationsContractTest;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Contract test for {@link SupportsAtomicOperations} against a zonal/Rapid GCS bucket. Validates
 * the single-request XML PUT CAS path end-to-end, including the concurrent-CAS race test that
 * asserts exactly one of N concurrent writers commits.
 *
 * <p>Set {@code RAPID_BUCKET} (and use ADC or {@code GOOGLE_APPLICATION_CREDENTIALS}) to run.
 * Skipped via {@link Assumptions} if {@code RAPID_BUCKET} is unset.
 *
 * <p>APPEND remains unsupported on GCS regardless of bucket type, so {@link #supportsAppend()}
 * returns the default ({@code false}).
 */
public class GcsFileIOAtomicRapidTest extends SupportsAtomicOperationsContractTest {

  private static final String RAPID_BUCKET_ENV = "RAPID_BUCKET";
  private static String rapidBucket;
  private static String runId;

  @BeforeAll
  static void readEnv() {
    rapidBucket = System.getenv(RAPID_BUCKET_ENV);
    runId = UUID.randomUUID().toString();
  }

  @BeforeEach
  void requireRapidBucket() {
    Assumptions.assumeTrue(
        rapidBucket != null && !rapidBucket.isEmpty(),
        "Real Rapid GCS bucket not configured; set " + RAPID_BUCKET_ENV);
  }

  @Override
  protected SupportsAtomicOperations newFileIO() {
    // GCSFileIO builds its own client via PrefixedStorage, as in production.
    GCSFileIO io = new GCSFileIO();
    Map<String, String> props = new HashMap<>();
    // ADC handles credentials. Project id is required for some metadata operations.
    String project = System.getenv("GOOGLE_CLOUD_PROJECT");
    if (project != null && !project.isEmpty()) {
      props.put(GCPProperties.GCS_PROJECT_ID, project);
    }
    io.initialize(props);
    return io;
  }

  @Override
  protected String randomLocation(String slug) {
    return String.format("gs://%s/%s/%s-%s", rapidBucket, runId, slug, UUID.randomUUID());
  }

  // ─── Regular (non-atomic) writes ────────────────────────────────────────────────────────────
  //
  // Zonal buckets reject resumable uploads, so GCSOutputFile.createOrOverwrite streams through an
  // appendable upload (GCSAppendableOutputStream) there. These cover what Iceberg writes that way:
  // manifests, data files, metadata JSON.

  /** A large file written in odd-sized pieces reads back intact, at the length written. */
  @Test
  void regularWriteStreamsALargeFile() throws IOException {
    String location = randomLocation("regular-large");
    SupportsAtomicOperations io = newFileIO();
    try {
      byte[] data = new byte[(20 << 20) + 12345];
      new Random(42).nextBytes(data);
      try (PositionOutputStream out = io.newOutputFile(location).createOrOverwrite()) {
        int off = 0;
        int step = 1;
        while (off < data.length) {
          int len = Math.min(step, data.length - off);
          out.write(data, off, len);
          off += len;
          step = step * 3 + 7; // 1, 10, 37, ... crosses every internal buffer boundary
        }
        assertThat(out.getPos()).isEqualTo(data.length);
      }
      InputFile in = io.newInputFile(location);
      assertThat(in.getLength()).isEqualTo(data.length);
      assertThat(read(in)).isEqualTo(data);
    } finally {
      cleanup(io, location);
    }
  }

  /** A file written by a regular write can be replaced by CAS, as a catalog's first file is. */
  @Test
  void regularWriteThenCas() throws IOException {
    String location = randomLocation("regular-then-cas");
    SupportsAtomicOperations io = newFileIO();
    try {
      try (PositionOutputStream out = io.newOutputFile(location).createOrOverwrite()) {
        out.write("v1".getBytes(StandardCharsets.UTF_8));
      }
      InputFile v1 = io.newInputFile(location);
      byte[] v2 = "v2".getBytes(StandardCharsets.UTF_8);
      AtomicOutputFile replace = io.newOutputFile(v1);
      CAS token =
          replace.prepare(() -> new ByteArrayInputStream(v2), AtomicOutputFile.Strategy.CAS);
      InputFile replaced = replace.writeAtomic(token, () -> new ByteArrayInputStream(v2));
      assertThat(read(replaced)).isEqualTo(v2);
    } finally {
      cleanup(io, location);
    }
  }

  /** {@code create()} refuses an existing object, as on standard buckets. */
  @Test
  void createRefusesAnExistingObject() throws IOException {
    String location = randomLocation("regular-create");
    SupportsAtomicOperations io = newFileIO();
    try {
      try (PositionOutputStream out = io.newOutputFile(location).create()) {
        out.write(1);
      }
      assertThatThrownBy(() -> io.newOutputFile(location).create())
          .isInstanceOf(AlreadyExistsException.class);
    } finally {
      cleanup(io, location);
    }
  }
}
