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

import org.apache.iceberg.encryption.NativeFileCryptoParameters;
import org.apache.iceberg.encryption.NativelyEncryptedFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class S3InputFile extends BaseS3File implements InputFile, NativelyEncryptedFile {
  private NativeFileCryptoParameters nativeDecryptionParameters;
  private Long length;
  private String etag;

  static S3InputFile fromLocation(
      String location, PrefixedS3Client client, MetricsContext metrics) {
    return fromLocation(location, 0, client, metrics);
  }

  static S3InputFile fromLocation(
      String location, long length, PrefixedS3Client client, MetricsContext metrics) {
    return new S3InputFile(
        client.s3(),
        client.s3FileIOProperties().isS3AnalyticsAcceleratorEnabled() ? client.s3Async() : null,
        new S3URI(location, client.s3FileIOProperties().bucketToAccessPointMapping()),
        length > 0 ? length : null,
        client.s3FileIOProperties(),
        metrics);
  }

  S3InputFile(
      S3Client client,
      S3AsyncClient asyncClient,
      S3URI uri,
      Long length,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics) {
    this(client, asyncClient, uri, length, s3FileIOProperties, metrics, null);
  }

  S3InputFile(
      S3Client client,
      S3AsyncClient asyncClient,
      S3URI uri,
      Long length,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics,
      String etag) {
    super(client, asyncClient, uri, s3FileIOProperties, metrics);
    this.length = length;
    this.etag = etag;
  }

  /** Returns the ETag for this input file, used for conditional writes. */
  public String etag() {
    if (etag == null && exists()) {
      this.etag = getObjectMetadata().eTag();
    }
    return etag;
  }

  /**
   * The ETag of the version this snapshot has seen, without a request: the one a write returned or
   * a lookup found, or null if it has seen none.
   */
  synchronized String seenEtag() {
    if (etag == null && cachedMetadata() != null) {
      this.etag = cachedMetadata().eTag();
    }
    return etag;
  }

  /** A lookup asks for the version this file reads, once a write or a read has chosen one. */
  @Override
  protected synchronized String lookupVersion() {
    return etag;
  }

  /**
   * Chooses the version a GET found, and its length, if the file has no version yet: what a request
   * first finds is what its later reads, its length and a replace see.
   */
  private synchronized void pin(GetObjectResponse response) {
    if (seenEtag() == null && response.eTag() != null) {
      this.etag = response.eTag();
      if (length == null) {
        this.length = objectLength(response);
      }
    }
  }

  /** The length of the object a GET's response describes, or null if it does not say. */
  static Long objectLength(GetObjectResponse response) {
    String range = response.contentRange();
    if (range == null) {
      // the whole object
      return response.contentLength();
    }
    String total = range.substring(range.lastIndexOf('/') + 1).trim();
    try {
      return "*".equals(total) ? null : Long.parseLong(total);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** The length this snapshot was given or has seen, without a request; null if neither. */
  Long knownLength() {
    if (length == null && cachedMetadata() != null) {
      this.length = cachedMetadata().contentLength();
    }
    return length;
  }

  /**
   * Note: this may be stale if file was deleted since metadata is cached for size/existence checks.
   *
   * @return content length
   */
  @Override
  public long getLength() {
    if (length == null) {
      this.length = getObjectMetadata().contentLength();
    }

    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    if (s3FileIOProperties().isS3AnalyticsAcceleratorEnabled()) {
      return AnalyticsAcceleratorUtil.newStream(this);
    }
    // The analytics accelerator's streams read whichever version is live: only S3InputStream
    // reads one version (SupportsAtomicOperations)
    return new S3InputStream(
        client(), uri(), s3FileIOProperties(), metrics(), this::seenEtag, this::pin);
  }

  @Override
  public NativeFileCryptoParameters nativeCryptoParameters() {
    return nativeDecryptionParameters;
  }

  @Override
  public void setNativeCryptoParameters(NativeFileCryptoParameters nativeCryptoParameters) {
    this.nativeDecryptionParameters = nativeCryptoParameters;
  }
}
