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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import org.apache.commons.codec.digest.PureJavaCrc32C;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.relocated.com.google.common.io.CharStreams;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

@ExtendWith(TestS3FileIOAtomic.SuccessCleanupExtension.class)
public class TestS3FileIOAtomic {
  // private static final Logger LOG = LoggerFactory.getLogger(TestS3FileIOAtomic.class);
  private static final String TEST_BUCKET = "casalog";

  private static S3Client s3;
  private static String uniqTestRun;
  private static String warehouseLocation;
  private static String warehousePath;

  @BeforeAll
  public static void initStorage() {
    // XXX integration tests are well designed, but I'd rather gargle yak piss than configure AWS.
    uniqTestRun = UUID.randomUUID().toString();
    // LOG.info("TEST RUN: {}", uniqTestRun);
    System.err.println("TEST RUN: " + uniqTestRun); // (logging disabled in tests)
    final AwsClientFactory clientFactory = AwsClientFactories.defaultFactory();
    s3 = clientFactory.s3();
    StaticClientFactory.client = s3;
  }

  @BeforeEach
  public void before(TestInfo info) {
    Assumptions.assumeTrue(s3 != null);
    final String testName = info.getTestMethod().orElseThrow(RuntimeException::new).getName();
    warehousePath = uniqTestRun + "/" + testName;
    warehouseLocation = "s3://" + TEST_BUCKET + "/" + warehousePath;
  }

  @AfterEach
  public void after() {
    // TODO
  }

  @Test
  public void testObjectPut() throws S3Exception {
    final String path = warehousePath + "/dingos";

    // write an object
    PutObjectRequest req1 = PutObjectRequest.builder().bucket(TEST_BUCKET).key(path).build();
    RequestBody body1 = RequestBody.fromBytes("ate my sandwich".getBytes(StandardCharsets.UTF_8));
    s3.putObject(req1, body1);
    PutObjectResponse resp1 = s3.putObject(req1, body1);

    // fail to overwrite it
    PutObjectRequest req2 =
        PutObjectRequest.builder().bucket(TEST_BUCKET).key(path).ifMatch("nope").build();
    RequestBody body2 = RequestBody.fromBytes("ate my tacos".getBytes(StandardCharsets.UTF_8));
    assertThatThrownBy(() -> s3.putObject(req2, body2))
        .isInstanceOf(S3Exception.class)
        .matches(e -> ((S3Exception) e).statusCode() == 412);

    // yup, still the same object
    HeadObjectRequest req3 = HeadObjectRequest.builder().bucket(TEST_BUCKET).key(path).build();
    assertThat(s3.headObject(req3)).extracting(HeadObjectResponse::eTag).isEqualTo(resp1.eTag());

    // overwrite w/ if-match
    PutObjectRequest req4 =
        PutObjectRequest.builder().bucket(TEST_BUCKET).key(path).ifMatch(resp1.eTag()).build();
    RequestBody body4 = RequestBody.fromBytes("ate my sushi".getBytes(StandardCharsets.UTF_8));
    PutObjectResponse resp4 = s3.putObject(req4, body4);

    // new object
    HeadObjectRequest req5 = HeadObjectRequest.builder().bucket(TEST_BUCKET).key(path).build();
    assertThat(s3.headObject(req5)).extracting(HeadObjectResponse::eTag).isEqualTo(resp4.eTag());
  }

  @Test
  public void testChecksum() throws S3Exception {
    final String path = warehousePath + "/wombats";

    final byte[] data = "cubed my pineapple".getBytes(StandardCharsets.UTF_8);

    final PureJavaCrc32C chk = new PureJavaCrc32C();
    chk.update(data, 0, data.length);
    String chkStr = Base64.getEncoder().encodeToString(Ints.toByteArray((int) chk.getValue()));

    PutObjectRequest req1 =
        PutObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(path)
            .checksumCRC32C(chkStr)
            .contentLength((long) data.length)
            .build();
    // RequestBody body1 = RequestBody.fromBytes(data);
    RequestBody body1 = RequestBody.fromInputStream(new ByteArrayInputStream(data), data.length);
    s3.putObject(req1, body1);
  }

  @Test
  public void testFileIOOverwrite() throws IOException, S3Exception {
    final String path = warehousePath + "/yaks";
    final String location = warehouseLocation + "/yaks";

    PutObjectRequest req1 = PutObjectRequest.builder().bucket(TEST_BUCKET).key(path).build();
    RequestBody body1 = RequestBody.fromBytes("shaved my kiwis".getBytes(StandardCharsets.UTF_8));
    s3.putObject(req1, body1);
    PutObjectResponse resp1 = s3.putObject(req1, body1);

    // let's see if this works
    S3FileIO fileIO = new S3FileIO(() -> s3);
    final InputFile inf = fileIO.newInputFile(location);
    try (InputStream i = inf.newStream()) {
      assertThat(CharStreams.toString(new InputStreamReader(i, StandardCharsets.UTF_8)))
          .isEqualTo("shaved my kiwis");
    }
    final AtomicOutputFile<CAS> outf = fileIO.newOutputFile(inf);
    final byte[] replContent = "shaved my hamster".getBytes(StandardCharsets.UTF_8);
    final CAS chk = outf.prepare(() -> new ByteArrayInputStream(replContent));

    InputFile replf = outf.writeAtomic(chk, () -> new ByteArrayInputStream(replContent));
    try (InputStream i = replf.newStream()) {
      assertThat(CharStreams.toString(new InputStreamReader(i, StandardCharsets.UTF_8)))
          .isEqualTo("shaved my hamster");
    }

    final AtomicOutputFile<CAS> outfFail = fileIO.newOutputFile(inf);
    final byte[] failContent = "shaved your mom".getBytes(StandardCharsets.UTF_8);
    final CAS chkFail = outfFail.prepare(() -> new ByteArrayInputStream(failContent));

    assertThatThrownBy(
            () -> outfFail.writeAtomic(chkFail, () -> new ByteArrayInputStream(failContent)))
        .isInstanceOf(SupportsAtomicOperations.CASException.class);
    try (InputStream i = replf.newStream()) {
      assertThat(CharStreams.toString(new InputStreamReader(i, StandardCharsets.UTF_8)))
          .isEqualTo("shaved my hamster");
    }
  }

  static class SuccessCleanupExtension implements TestWatcher {
    @Override
    public void testSuccessful(ExtensionContext ctxt) {
      cleanupWarehouseLocation();
    }
  }

  static void cleanupWarehouseLocation() {
    // use FileIO
  }
}
