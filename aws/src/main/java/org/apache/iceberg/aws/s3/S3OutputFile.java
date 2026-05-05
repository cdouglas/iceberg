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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.function.Supplier;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.iceberg.encryption.NativeFileCryptoParameters;
import org.apache.iceberg.encryption.NativelyEncryptedFile;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.FileChecksumOutputStream;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.InvalidWriteOffsetException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3OutputFile extends BaseS3File implements AtomicOutputFile, NativelyEncryptedFile {
  private NativeFileCryptoParameters nativeEncryptionParameters;
  // Atomic-write precondition: when non-null, ifMatch=etag pins the live object; when null and
  // assertAbsent=true, ifNoneMatch=* requires the destination to not exist; when both are unset
  // (vanilla overwrite path), no precondition is applied.
  private final String etag;
  private final boolean assertAbsent;

  /**
   * Creates a {@link S3OutputFile} from the given parameters.
   *
   * @deprecated since 1.10.0, will be removed in 1.11.0; use {@link
   *     S3OutputFile#fromLocation(String, PrefixedS3Client, MetricsContext)} instead.
   */
  @Deprecated
  public static S3OutputFile fromLocation(
      String location,
      S3Client client,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics) {
    return new S3OutputFile(
        client,
        null,
        new S3URI(location, s3FileIOProperties.bucketToAccessPointMapping()),
        s3FileIOProperties,
        metrics,
        null);
  }

  /**
   * Creates a {@link S3OutputFile} from the given parameters.
   *
   * @deprecated since 1.10.0, will be removed in 1.11.0; use {@link
   *     S3OutputFile#fromLocation(String, PrefixedS3Client, MetricsContext)} instead.
   */
  @Deprecated
  public static S3OutputFile fromLocation(
      String location,
      S3Client client,
      S3AsyncClient asyncClient,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics) {
    return new S3OutputFile(
        client,
        asyncClient,
        new S3URI(location, s3FileIOProperties.bucketToAccessPointMapping()),
        s3FileIOProperties,
        metrics,
        null);
  }

  static S3OutputFile fromLocation(
      String location, PrefixedS3Client client, MetricsContext metrics) {
    return fromLocation(location, client, metrics, null, false);
  }

  static S3OutputFile fromLocation(
      String location, PrefixedS3Client client, MetricsContext metrics, String etag) {
    return fromLocation(location, client, metrics, etag, false);
  }

  static S3OutputFile fromLocation(
      String location,
      PrefixedS3Client client,
      MetricsContext metrics,
      String etag,
      boolean assertAbsent) {
    return new S3OutputFile(
        client.s3(),
        client.s3FileIOProperties().isS3AnalyticsAcceleratorEnabled() ? client.s3Async() : null,
        new S3URI(location, client.s3FileIOProperties().bucketToAccessPointMapping()),
        client.s3FileIOProperties(),
        metrics,
        etag,
        assertAbsent);
  }

  S3OutputFile(
      S3Client client,
      S3AsyncClient asyncClient,
      S3URI uri,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics,
      String etag) {
    this(client, asyncClient, uri, s3FileIOProperties, metrics, etag, false);
  }

  S3OutputFile(
      S3Client client,
      S3AsyncClient asyncClient,
      S3URI uri,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics,
      String etag,
      boolean assertAbsent) {
    super(client, asyncClient, uri, s3FileIOProperties, metrics);
    this.etag = etag;
    this.assertAbsent = assertAbsent;
  }

  /**
   * Create an output stream for the specified location if the target object does not exist in S3 at
   * the time of invocation.
   *
   * @return output stream
   */
  @Override
  public PositionOutputStream create() {
    if (!exists()) {
      return createOrOverwrite();
    } else {
      throw new AlreadyExistsException("Location already exists: %s", uri());
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    try {
      return new S3OutputStream(client(), uri(), s3FileIOProperties(), metrics());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create output stream for location: " + uri(), e);
    }
  }

  @Override
  public InputFile toInputFile() {
    return new S3InputFile(
        client(), asyncClient(), uri(), null, s3FileIOProperties(), metrics(), etag);
  }

  @Override
  public NativeFileCryptoParameters nativeCryptoParameters() {
    return nativeEncryptionParameters;
  }

  @Override
  public void setNativeCryptoParameters(NativeFileCryptoParameters nativeCryptoParameters) {
    this.nativeEncryptionParameters = nativeCryptoParameters;
  }

  @Override
  public CAS prepare(Supplier<InputStream> source, Strategy howto) throws IOException {
    // S3OutputStream forces multipart upload w/ attendant MD5 and work pool... meh, do it manually
    // in writeAtomic. Catalog + metadata writes are likely smaller than multipart would justify.
    final S3Checksum checksum = new S3Checksum(howto);
    try (InputStream in = source.get();
        FileChecksumOutputStream chk =
            new FileChecksumOutputStream(NullOutputStream.INSTANCE, checksum)) {
      ByteStreams.copy(in, chk);
    }
    return checksum;
  }

  @Override
  public InputFile writeAtomic(CAS token, Supplier<InputStream> source) throws IOException {
    final S3Checksum tok = (S3Checksum) token;
    switch (tok.getStrategy()) {
      case CAS:
        return replaceDestObj(tok, source);
      case APPEND:
        return appendDestObj(tok, source);
      default:
        throw new UnsupportedOperationException("Unrecognized strategy: " + tok.getStrategy());
    }
  }

  private S3InputFile replaceDestObj(S3Checksum token, Supplier<InputStream> source)
      throws IOException {
    try (InputStream src = source.get()) {
      final S3URI location = uri();
      PutObjectRequest.Builder reqBuilder =
          PutObjectRequest.builder()
              .bucket(location.bucket())
              .key(location.key())
              .checksumCRC32C(token.contentHeaderString())
              .contentLength(token.contentLength());
      // Atomic-write precondition: ifMatch pins a known etag; otherwise ifNoneMatch=* enforces
      // "object must not exist." A null etag with assertAbsent=false indicates a vanilla overwrite
      // and applies no precondition.
      if (etag != null) {
        reqBuilder.ifMatch(etag);
      } else if (assertAbsent) {
        reqBuilder.ifNoneMatch("*");
      }
      PutObjectRequest req = reqBuilder.build();
      RequestBody content = RequestBody.fromInputStream(src, token.contentLength());
      PutObjectResponse response = client().putObject(req, content);
      return new S3InputFile(
          client(),
          asyncClient(),
          location,
          token.contentLength(),
          s3FileIOProperties(),
          metrics(),
          response.eTag());
    } catch (S3Exception e) {
      if (409 == e.statusCode()
          && "ConditionalRequestConflict".equals(e.awsErrorDetails().errorCode())) {
        throw new SupportsAtomicOperations.CASException("Conflicting operation", e);
      }
      if (412 == e.statusCode() && "PreconditionFailed".equals(e.awsErrorDetails().errorCode())) {
        throw new SupportsAtomicOperations.CASException("Target modified", e);
      }
      throw e;
    }
  }

  private S3InputFile appendDestObj(S3Checksum token, Supplier<InputStream> source)
      throws IOException {
    final long objLength = getObjectMetadata().contentLength();
    try (InputStream src = source.get()) {
      final S3URI location = uri();
      PutObjectRequest req =
          PutObjectRequest.builder()
              .bucket(location.bucket())
              .key(location.key())
              .checksumCRC32C(token.contentHeaderString())
              .contentLength(token.contentLength())
              .ifMatch(etag)
              .writeOffsetBytes(objLength)
              .build();
      RequestBody content = RequestBody.fromInputStream(src, token.contentLength());
      PutObjectResponse response = client().putObject(req, content);
      return new S3InputFile(
          client(),
          asyncClient(),
          location,
          objLength + token.contentLength(),
          s3FileIOProperties(),
          metrics(),
          response.eTag());
    } catch (InvalidWriteOffsetException e) {
      throw new SupportsAtomicOperations.AppendException("Wrong offset", e);
    } catch (S3Exception e) {
      if (409 == e.statusCode()
          && "ConditionalRequestConflict".equals(e.awsErrorDetails().errorCode())) {
        throw new SupportsAtomicOperations.AppendException("Conflicting operation", e);
      }
      if (412 == e.statusCode() && "PreconditionFailed".equals(e.awsErrorDetails().errorCode())) {
        throw new SupportsAtomicOperations.AppendException("Target modified", e);
      }
      throw e;
    }
  }
}
