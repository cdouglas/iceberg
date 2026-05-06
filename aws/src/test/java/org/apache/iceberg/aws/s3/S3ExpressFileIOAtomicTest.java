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
package org.apache.iceberg.aws.s3;

import java.util.UUID;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.io.SupportsAtomicOperationsContractTest;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * S3 Express One Zone (directory bucket) implementation of the {@link
 * SupportsAtomicOperationsContractTest}. APPEND is supported via {@code writeOffsetBytes}.
 */
public class S3ExpressFileIOAtomicTest extends SupportsAtomicOperationsContractTest {

  private static final String EXPR_BUCKET =
      System.getenv().getOrDefault("S3_EXPRESS_TEST_BUCKET", "lst-pbafvfgrapl--usw2-az3--x-s3");
  private static String runId;
  private static S3Client s3;

  @BeforeAll
  static void initStorage() {
    String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    if (accessKey != null && secretKey != null) {
      runId = UUID.randomUUID().toString();
      s3 = AwsClientFactories.defaultFactory().s3();
    }
  }

  @BeforeEach
  void requireCredentials() {
    Assumptions.assumeTrue(s3 != null, "AWS credentials not available");
  }

  @Override
  protected SupportsAtomicOperations newFileIO() {
    S3FileIO io = new S3FileIO(() -> s3);
    io.initialize(Maps.newHashMap());
    return io;
  }

  @Override
  protected String randomLocation(String slug) {
    return String.format("s3://%s/%s-%s-%s", EXPR_BUCKET, runId, slug, UUID.randomUUID());
  }
}
