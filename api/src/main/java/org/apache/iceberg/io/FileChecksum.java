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
package org.apache.iceberg.io;

/**
 * Checksum API for object stores. Annoyingly, Java does not have a common interface for CRC32 and
 * MD5. The convoluted "obtain a checksum object from the outputfile and use it to write" API sucks,
 * but whatever, it'll suffice for experiments.
 */
public interface FileChecksum {
  // TODO include a path to provide a checksum (known locally), rather than computing it

  default void update(int onebyte) {
    update(new byte[] {(byte) onebyte}, 0, 1);
  }

  default void update(byte[] bytes) {
    update(bytes, 0, bytes.length);
  }

  long contentLength();

  void update(byte[] bytes, int off, int len);

  byte[] asBytes();

  String toHeaderString();
}
