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
package org.apache.iceberg.benchmark.compaction;

import java.io.IOException;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shells out to {@code aws s3 cp} for tarball upload. Setup runs on the implementer's workstation
 * which is expected to already have the AWS CLI configured (per spec §"Cloud Configuration"); we
 * deliberately avoid pulling in the AWS Java SDK to keep this module's dependency surface small.
 *
 * <p>Returns the destination S3 URI on success. Throws {@link IOException} on a non-zero exit code
 * so {@link SetupMain} can fail loudly rather than silently producing a broken manifest.
 */
public final class S3Uploader {

  private static final Logger LOG = LoggerFactory.getLogger(S3Uploader.class);

  private S3Uploader() {}

  /** Upload {@code localFile} to {@code s3Uri}. */
  public static String upload(Path localFile, String s3Uri) throws IOException {
    LOG.info("Uploading {} ({} bytes) -> {}", localFile, sizeOf(localFile), s3Uri);
    ProcessBuilder pb =
        new ProcessBuilder("aws", "s3", "cp", localFile.toAbsolutePath().toString(), s3Uri)
            .inheritIO();
    Process process = pb.start();
    int exit;
    try {
      exit = process.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for `aws s3 cp` to finish", e);
    }
    if (exit != 0) {
      throw new IOException(
          "`aws s3 cp " + localFile + " " + s3Uri + "` exited with status " + exit);
    }
    return s3Uri;
  }

  private static long sizeOf(Path file) {
    try {
      return java.nio.file.Files.size(file);
    } catch (IOException e) {
      return -1L;
    }
  }
}
