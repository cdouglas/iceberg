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

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelineNextSyncPolicy;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.AzureSasCredentialPolicy;
import com.azure.core.http.policy.HttpPipelinePolicy;
import reactor.core.publisher.Mono;

/**
 * Signs requests with the SAS token vended for the account, fetched (and refreshed) per request.
 *
 * <p>Besides the request URL, it signs the source of a Data Lake rename. A rename names its source
 * in {@code x-ms-rename-source}, and under SAS auth the service authorizes the source with a SAS in
 * that header, separately from the request. The SDK adds one only for clients built with {@code
 * sasToken(...)}; with the SAS added here instead, the header would go out bare and the rename
 * would fail with 403 {@code AuthorizationPermissionMismatch}, however the token is scoped.
 * ADLSFileIO's CAS renames a staged temp onto the target, so it needs this.
 */
class VendedAzureSasCredentialPolicy implements HttpPipelinePolicy {
  private static final HttpHeaderName RENAME_SOURCE =
      HttpHeaderName.fromString("x-ms-rename-source");

  private final String account;
  private final VendedAdlsCredentialProvider vendedAdlsCredentialProvider;
  private volatile AzureSasCredential azureSasCredential;
  private AzureSasCredentialPolicy azureSasCredentialPolicy;

  VendedAzureSasCredentialPolicy(
      String account, VendedAdlsCredentialProvider vendedAdlsCredentialProvider) {
    this.account = account;
    this.vendedAdlsCredentialProvider = vendedAdlsCredentialProvider;
  }

  @Override
  public Mono<HttpResponse> process(
      HttpPipelineCallContext httpPipelineCallContext,
      HttpPipelineNextPolicy httpPipelineNextPolicy) {
    return maybeUpdateCredential()
        .then(
            Mono.defer(
                () -> {
                  signRenameSource(httpPipelineCallContext.getHttpRequest());
                  return azureSasCredentialPolicy.process(
                      httpPipelineCallContext, httpPipelineNextPolicy);
                }));
  }

  @Override
  public HttpResponse processSync(
      HttpPipelineCallContext context, HttpPipelineNextSyncPolicy next) {
    maybeUpdateCredential().block();
    signRenameSource(context.getHttpRequest());
    return azureSasCredentialPolicy.processSync(context, next);
  }

  /**
   * Appends the current SAS to {@code x-ms-rename-source}, replacing any query already there: the
   * SDK leaves the header unsigned for clients without a SAS token of their own, and a retry after
   * the token rotated must not carry the old one.
   */
  private void signRenameSource(HttpRequest request) {
    String source = request.getHeaders().getValue(RENAME_SOURCE);
    if (source == null) {
      return;
    }
    int query = source.indexOf('?');
    String path = query < 0 ? source : source.substring(0, query);
    String sas = azureSasCredential.getSignature();
    request.setHeader(RENAME_SOURCE, path + "?" + (sas.startsWith("?") ? sas.substring(1) : sas));
  }

  private Mono<Void> maybeUpdateCredential() {
    return vendedAdlsCredentialProvider
        .credentialForAccount(account)
        .flatMap(
            sasToken -> {
              if (azureSasCredential == null) {
                synchronized (this) {
                  if (azureSasCredential == null) {
                    this.azureSasCredential = new AzureSasCredential(sasToken);
                    this.azureSasCredentialPolicy =
                        new AzureSasCredentialPolicy(azureSasCredential, false);
                    return Mono.empty();
                  }
                }
              }
              azureSasCredential.update(sasToken);
              return Mono.empty();
            });
  }
}
