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
import org.apache.iceberg.encryption.NativeFileCryptoParameters;
import org.apache.iceberg.encryption.NativelyEncryptedFile;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.FileChecksum;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SupportsAtomicOperations;
import org.apache.iceberg.metrics.MetricsContext;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3OutputFile extends BaseS3File
    implements OutputFile, NativelyEncryptedFile, AtomicOutputFile {
  private NativeFileCryptoParameters nativeEncryptionParameters;
  private final String etag;

  public static S3OutputFile fromLocation(
      String location,
      S3Client client,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics) {
    return new S3OutputFile(
        client,
        new S3URI(location, s3FileIOProperties.bucketToAccessPointMapping()),
        s3FileIOProperties,
        metrics);
  }

  S3OutputFile(
      S3Client client, S3URI uri, S3FileIOProperties s3FileIOProperties, MetricsContext metrics) {
    this(client, uri, s3FileIOProperties, metrics, null);
  }

  S3OutputFile(
      S3Client client,
      S3URI uri,
      S3FileIOProperties s3FileIOProperties,
      MetricsContext metrics,
      String etag) {
    super(client, uri, s3FileIOProperties, metrics);
    this.etag = etag;
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
    return new S3InputFile(client(), uri(), null, s3FileIOProperties(), metrics(), etag);
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
  public FileChecksum checksum() {
    // S3OutputStream forces multipart upload w/ attendant MD5 and work pool... meh, do it manually
    // in writeAtomic
    // Catalog + metadata writes are likely smaller than multipart would justify, anyway
    return new S3Checksum();
  }

  @Override
  public InputFile writeAtomic(FileChecksum checksum, Supplier<InputStream> source)
      throws IOException {
    try (InputStream src = source.get()) {
      final S3URI location = uri();
      PutObjectRequest req =
          PutObjectRequest.builder()
              .bucket(location.bucket())
              .key(location.key())
              .checksumCRC32C(checksum.toHeaderString())
              .contentLength(checksum.contentLength())
              .ifMatch(etag)
              .build();
      RequestBody content = RequestBody.fromInputStream(src, checksum().contentLength());
      PutObjectResponse response = client().putObject(req, content);
      return new S3InputFile(
          client(),
          location,
          checksum.contentLength(),
          s3FileIOProperties(),
          metrics(),
          response.eTag());
    } catch (S3Exception e) {
      if (412 == e.statusCode()) {
        // precondition failed
        throw new SupportsAtomicOperations.CASException("Target modified", e);
      }
      throw e;
    }
  }
}
