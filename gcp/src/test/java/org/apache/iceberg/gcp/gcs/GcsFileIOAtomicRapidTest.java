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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.io.SupportsAtomicOperationsContractTest;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

/**
 * Contract test for {@link SupportsAtomicOperations} against a zonal/Rapid GCS bucket. Validates
 * the stage-and-move CAS path end-to-end, including the concurrent-CAS race test that asserts
 * exactly one of N concurrent writers commits.
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
    // GCSFileIO must build its own clients (HTTP + gRPC) via PrefixedStorage. Providing a
    // pre-built Storage supplier would prevent the gRPC client from materializing.
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
}
