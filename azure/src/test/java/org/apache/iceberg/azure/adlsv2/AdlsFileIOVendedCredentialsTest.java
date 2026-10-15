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
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.azure.core.http.HttpMethod;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponseParser;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.model.HttpRequest;
import org.mockserver.verify.VerificationTimes;

/**
 * ADLSFileIO's atomic writes against real ADLS with vended credentials: the FileIO is configured
 * only with a credentials endpoint (a local mock REST server), which vends the SAS token for the
 * account. CAS renames a staged temp onto the target, and under SAS auth the rename source must
 * carry the SAS; {@link VendedAzureSasCredentialPolicy} adds it.
 *
 * <p>Set {@code AZURE_STORAGE_ACCOUNT}, {@code AZURE_STORAGE_SAS_TOKEN} and {@code
 * AZURE_TEST_CONTAINER} to run; skipped otherwise.
 */
public class AdlsFileIOVendedCredentialsTest extends VendedCredentialsTestBase {
  private static final HttpRequest CREDENTIALS_REQUEST =
      request("/v1/credentials").withMethod(HttpMethod.GET.name());

  private String account;
  private String container;
  private String sasToken;

  @BeforeEach
  void vendTheRealToken() {
    account = System.getenv("AZURE_STORAGE_ACCOUNT");
    container = System.getenv("AZURE_TEST_CONTAINER");
    sasToken = System.getenv("AZURE_STORAGE_SAS_TOKEN");
    Assumptions.assumeTrue(
        account != null && !account.isBlank() && container != null && sasToken != null,
        "ADLS not available - set AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_SAS_TOKEN, AZURE_TEST_CONTAINER");

    // The policy looks the token up by host, as ADLSFileIO passes it.
    String host = account + ".dfs.core.windows.net";
    String body =
        LoadCredentialsResponseParser.toJson(
            ImmutableLoadCredentialsResponse.builder()
                .addCredentials(
                    ImmutableCredential.builder()
                        .prefix(String.format("abfs://%s@%s", container, host))
                        .config(
                            ImmutableMap.of(
                                AzureProperties.ADLS_SAS_TOKEN_PREFIX + host,
                                sasToken.startsWith("?") ? sasToken.substring(1) : sasToken,
                                AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + host,
                                Long.toString(System.currentTimeMillis() + 3_600_000L)))
                        .build())
                .build());
    mockServer.when(CREDENTIALS_REQUEST).respond(response(body).withStatusCode(200));
  }

  /** Create, append, replace and append again, with no credential but the vended one. */
  @Test
  void casAndAppendWithVendedSas() throws IOException {
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.URI,
            baseUri + "/v1/",
            AzureProperties.ADLS_REFRESH_CREDENTIALS_ENDPOINT,
            baseUri + "/v1/credentials");
    ADLSFileIO io = new ADLSFileIO(properties);
    String location =
        String.format(
            "abfs://%s@%s.dfs.core.windows.net/%s/vended-cas",
            container, account, UUID.randomUUID());
    try {
      InputFile current =
          write(io, io.newInputFile(location), "ckpt|", AtomicOutputFile.Strategy.CAS);
      current = write(io, current, "r1|", AtomicOutputFile.Strategy.APPEND);
      current = write(io, current, "ckpt2|", AtomicOutputFile.Strategy.CAS);
      current = write(io, current, "r2|", AtomicOutputFile.Strategy.APPEND);
      assertThat(read(io.newInputFile(location))).isEqualTo("ckpt2|r2|");
      mockServer.verify(CREDENTIALS_REQUEST, VerificationTimes.atLeast(1));
    } finally {
      io.deleteFile(location);
    }
  }

  private static InputFile write(
      ADLSFileIO io, InputFile snapshot, String content, AtomicOutputFile.Strategy strategy)
      throws IOException {
    byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    AtomicOutputFile out = io.newOutputFile(snapshot);
    CAS token = out.prepare(() -> new ByteArrayInputStream(bytes), strategy);
    return out.writeAtomic(token, () -> new ByteArrayInputStream(bytes));
  }

  private static String read(InputFile file) throws IOException {
    try (InputStream in = file.newStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
