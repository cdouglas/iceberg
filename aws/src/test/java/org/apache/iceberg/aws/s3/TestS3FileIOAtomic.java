/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.aws.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsClientFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestS3FileIOAtomic.SuccessCleanupExtension.class)
public class TestS3FileIOAtomic {
    private static final Logger LOG = LoggerFactory.getLogger(TestS3FileIOAtomic.class);
    private static final String TEST_BUCKET = "casalog";

    // TODO: may need to use StaticClientFactory
    private static S3Client s3;
    private static String uniqTestRun;
    private static String warehouseLocation;
    private static String warehousePath;

    @BeforeAll
    public static void initStorage() {
        // XXX integration tests are well designed, but I'd rather gargle yak piss than configure AWS.
        uniqTestRun = UUID.randomUUID().toString();
        LOG.info("TEST RUN: " + uniqTestRun);
        try {
            final AwsClientFactory clientFactory = AwsClientFactories.defaultFactory();
            s3 = clientFactory.s3();
            StaticClientFactory.client = s3;
        } catch (Exception ignored) {}
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
        PutObjectRequest req2 = PutObjectRequest.builder().bucket(TEST_BUCKET).key(path).ifMatch("nope").build();
        RequestBody body2 = RequestBody.fromBytes("ate my tacos".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> s3.putObject(req2, body2))
                .isInstanceOf(S3Exception.class)
                .matches(e -> ((S3Exception)e).statusCode() == 412);

        // yup, still the same object
        HeadObjectRequest req3 = HeadObjectRequest.builder().bucket(TEST_BUCKET).key(path).build();
        assertThat(s3.headObject(req3)).extracting(HeadObjectResponse::eTag).isEqualTo(resp1.eTag());

        // overwrite w/ if-match
        PutObjectRequest req4 = PutObjectRequest.builder().bucket(TEST_BUCKET).key(path).ifMatch(resp1.eTag()).build();
        RequestBody body4 = RequestBody.fromBytes("ate my sushi".getBytes(StandardCharsets.UTF_8));
        s3.putObject(req4, body4);
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
