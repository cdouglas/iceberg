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

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.io.SupportsAtomicOperationsContractTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * GCS implementation of the {@link SupportsAtomicOperationsContractTest}. APPEND is not exposed.
 */
public class GcsFileIOAtomicTest extends SupportsAtomicOperationsContractTest {

  private static final String TEST_BUCKET = "lst-consistency";
  private static String runId;
  private static Storage storage;

  @BeforeAll
  static void initStorage() throws IOException {
    runId = UUID.randomUUID().toString();
    String credPath =
        System.getenv()
            .getOrDefault(
                "GOOGLE_APPLICATION_CREDENTIALS",
                "/IdeaProjects/.cloud/gcs/lst-consistency-8dd2dfbea73a.json");
    File credFile = new File(credPath);
    if (credFile.exists()) {
      try (FileInputStream creds = new FileInputStream(credFile)) {
        storage = RemoteStorageHelper.create(TEST_BUCKET, creds).getOptions().getService();
      }
    }
  }

  @BeforeEach
  void requireRealStorage() {
    Assumptions.assumeTrue(
        storage != null, "Real GCS not available - set GOOGLE_APPLICATION_CREDENTIALS");
  }

  @Override
  protected SupportsAtomicOperations newFileIO() {
    return new GCSFileIO(() -> storage, new GCPProperties());
  }

  @Override
  protected String randomLocation(String slug) {
    return String.format("gs://%s/%s/%s-%s", TEST_BUCKET, runId, slug, UUID.randomUUID());
  }

  // ─── GCS-specific behavior tests ────────────────────────────────────────────────────────────

  /**
   * GCS-specific: a CAS write whose stream is short of the prepared content length triggers
   * server-side CRC32C validation failure. The contract test only covers the success path; this
   * verifies the SDK enforces the integrity check end-to-end on a partial write.
   */
  @Test
  void partialWriteRejectedByCrc32cMismatch() throws IOException {
    Random random = new Random();
    String location = randomLocation("partial-write");
    SupportsAtomicOperations io = newFileIO();
    byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);

    OutputFile out = ((GCSFileIO) io).newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(expected));
    }

    InputFile in = io.newInputFile(location);
    assertThat(in.exists()).isTrue();

    AtomicOutputFile overwrite = io.newOutputFile(in);
    byte[] overbytes = new byte[1024 * 1024];
    random.nextBytes(overbytes);
    CAS chk =
        overwrite.prepare(() -> new ByteArrayInputStream(overbytes), AtomicOutputFile.Strategy.CAS);

    StorageException failure =
        Assertions.assertThrows(
            StorageException.class,
            () -> {
              // partial write: stream cut to half the prepared content length
              overwrite.writeAtomic(chk, () -> new ByteArrayInputStream(overbytes, 0, 512 * 1024));
            });
    assertThat(failure.getCause().getMessage())
        .containsPattern(
            Pattern.compile(
                "Provided CRC32C \\\\\"[A-Za-z0-9+/=]+\\\\\" doesn't match calculated CRC32C \\\\\"[A-Za-z0-9+/=]+\\\\\""));
  }
}
