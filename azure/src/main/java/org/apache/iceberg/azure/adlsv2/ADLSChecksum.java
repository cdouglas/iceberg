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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import org.apache.iceberg.io.FileChecksum;

public class ADLSChecksum implements FileChecksum {
  private long length = 0L;
  private final MessageDigest checksum = getMD5DigestInstance();

  private static MessageDigest getMD5DigestInstance() {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("Missing MD5 implementation");
    }
  }

  @Override
  public long contentLength() {
    return length;
  }

  @Override
  public void update(byte[] bytes, int off, int len) {
    checksum.update(bytes, off, len);
    length += len;
  }

  @Override
  public byte[] asBytes() {
    return checksum.digest();
  }

  @Override
  public String toHeaderString() {
    return Base64.getEncoder().encodeToString(asBytes());
  }

  @Override
  public String toString() {
    return toHeaderString();
  }
}
