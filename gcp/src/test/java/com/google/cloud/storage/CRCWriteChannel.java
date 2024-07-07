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
package com.google.cloud.storage;

import com.google.api.core.ApiFuture;
import com.google.cloud.RestorableState;
import com.google.cloud.WriteChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.zip.Checksum;
import org.apache.commons.codec.digest.PureJavaCrc32C;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;

public class CRCWriteChannel implements StorageWriteChannel {

  private final WriteChannel out;
  private final long crc32c;
  private final Checksum actual = new PureJavaCrc32C();

  public CRCWriteChannel(WriteChannel out, long crc32c) {
    this.out = out;
    this.crc32c = crc32c;
  }

  @Override
  public void setChunkSize(int i) {
    out.setChunkSize(i);
  }

  @Override
  public RestorableState<WriteChannel> capture() {
    return out.capture();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    actual.update(src.array(), src.position(), src.remaining());
    return out.write(src);
  }

  @Override
  public boolean isOpen() {
    return out.isOpen();
  }

  @Override
  public void close() throws IOException {
    if (crc32c != actual.getValue()) {
      String expectedStr = Base64.getEncoder().encodeToString(Ints.toByteArray((int) crc32c));
      String actualStr =
          Base64.getEncoder().encodeToString(Ints.toByteArray((int) actual.getValue()));
      throw new StorageException(
          400,
          "Wrapped StorageException",
          new StorageException(
              400,
              String.format(
                  "Provided CRC32C \\\"%s\\\" doesn't match calculated CRC32C \\\"%s\\\"",
                  expectedStr, actualStr)));
    }
    out.close();
  }

  @Override
  public ApiFuture<BlobInfo> getObject() {
    throw new UnsupportedOperationException("FFS");
  }
}
