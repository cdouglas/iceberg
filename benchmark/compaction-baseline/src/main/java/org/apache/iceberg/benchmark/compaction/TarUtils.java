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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

/**
 * Tarball helper for {@link SetupMain}. Uses Apache Commons Compress (transitively present via
 * Hadoop) so behavior is the same on Linux/macOS/Windows and doesn't depend on the host's {@code
 * tar} binary.
 *
 * <p>Tars are uncompressed by design — the warehouse contains Snappy-encoded Parquet data files
 * which are already incompressible, so wrapping in gzip would only add CPU cost.
 */
public final class TarUtils {

  private TarUtils() {}

  /**
   * Tar the contents of {@code sourceDir} into {@code tarPath}. The root entry name in the archive
   * is {@code archiveRootName}; subdirectories preserve their relative paths under that root.
   */
  public static void tarDirectory(Path sourceDir, Path tarPath, String archiveRootName)
      throws IOException {
    Files.createDirectories(tarPath.toAbsolutePath().getParent());
    try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(tarPath));
        TarArchiveOutputStream tar = new TarArchiveOutputStream(out)) {
      tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
      tar.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);

      try (Stream<Path> walk = Files.walk(sourceDir)) {
        for (Path path : (Iterable<Path>) walk::iterator) {
          addTarEntry(tar, sourceDir, path, archiveRootName);
        }
      }
      tar.finish();
    }
  }

  private static void addTarEntry(
      TarArchiveOutputStream tar, Path sourceDir, Path path, String archiveRootName)
      throws IOException {
    Path relative = sourceDir.relativize(path);
    if (relative.toString().isEmpty()) {
      return;
    }
    String entryName = archiveRootName + "/" + relative.toString().replace('\\', '/');
    if (Files.isDirectory(path)) {
      entryName = entryName + "/";
    }
    TarArchiveEntry entry = new TarArchiveEntry(path.toFile(), entryName);
    tar.putArchiveEntry(entry);
    if (Files.isRegularFile(path)) {
      try (InputStream in = new BufferedInputStream(Files.newInputStream(path))) {
        in.transferTo(tar);
      }
    }
    tar.closeArchiveEntry();
  }

  /**
   * Untar {@code tarPath} into {@code targetDir}. Existing files are overwritten. Used by tests;
   * the production runner uses {@code aws s3 cp} + {@code tar -xf} on the VM.
   */
  public static void untar(Path tarPath, Path targetDir) throws IOException {
    Files.createDirectories(targetDir);
    try (InputStream in = new BufferedInputStream(Files.newInputStream(tarPath));
        TarArchiveInputStream tar = new TarArchiveInputStream(in)) {
      TarArchiveEntry entry;
      while ((entry = tar.getNextEntry()) != null) {
        Path out = targetDir.resolve(entry.getName()).normalize();
        // Defense-in-depth against tarball path traversal even though we authored these archives
        // ourselves.
        if (!out.startsWith(targetDir)) {
          throw new IOException("Refusing to extract entry outside target dir: " + entry.getName());
        }
        if (entry.isDirectory()) {
          Files.createDirectories(out);
        } else {
          Files.createDirectories(out.getParent());
          try (OutputStream fileOut = new BufferedOutputStream(Files.newOutputStream(out))) {
            tar.transferTo(fileOut);
          }
          if (out.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            Set<PosixFilePermission> perms =
                PosixFilePermissions.fromString(perms(entry.getMode()));
            Files.setPosixFilePermissions(out, perms);
          }
        }
      }
    }
  }

  /** Convert a POSIX numeric mode (e.g., 0644) to the symbolic form expected by NIO. */
  private static String perms(int mode) {
    char[] out = "---------".toCharArray();
    char[] rwx = {'r', 'w', 'x'};
    for (int who = 0; who < 3; who++) {
      int shift = (2 - who) * 3;
      for (int bit = 0; bit < 3; bit++) {
        if ((mode >> (shift + 2 - bit) & 1) != 0) {
          out[who * 3 + bit] = rwx[bit];
        }
      }
    }
    return new String(out);
  }
}
