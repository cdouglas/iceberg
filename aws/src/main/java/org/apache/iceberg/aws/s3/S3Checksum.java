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

import java.util.Base64;
import java.util.zip.Checksum;
import org.apache.commons.codec.digest.PureJavaCrc32C;
import org.apache.iceberg.io.CAS;
import org.apache.iceberg.io.FileChecksum;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;

// mild abuse of types to accommodate existing work
public class S3Checksum implements FileChecksum, CAS {

  private long length = 0L;
  private final Checksum crc32c = new PureJavaCrc32C();

  @Override
  public void update(byte[] bytes, int off, int len) {
    crc32c.update(bytes, off, len);
    length += len;
  }

  @Override
  public long contentLength() {
    return length;
  }

  @Override
  public byte[] contentChecksumBytes() {
    return Ints.toByteArray((int) crc32c.getValue());
  }

  @Override
  public String contentHeaderString() {
    return Base64.getEncoder().encodeToString(contentChecksumBytes());
  }

  @Override
  public String toString() {
    return contentHeaderString();
  }
}
