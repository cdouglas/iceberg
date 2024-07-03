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
package org.apache.iceberg.gcp.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.CRCWriteChannel;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.AtomicOutputFile;
import org.apache.iceberg.io.FileChecksum;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSFileIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(GCSFileIOTest.class);
  private static String uniqTestRun = UUID.randomUUID().toString();
  private static final String TEST_BUCKET = "lst-consistency";
  private final Random random = new Random(1);

  private static Storage storage;
  private String testName;
  private GCSFileIO io;

  @BeforeAll
  public static void initStorage() throws IOException {
    uniqTestRun = UUID.randomUUID().toString();
    LOG.info("TEST RUN: " + uniqTestRun);
    // TODO get from env
    final File credFile = new File("/home/xchris/work/.cloud/gcp/lst-consistency-8dd2dfbea73a.json");
    // final File credFile =
    //     new File("/IdeaProjects/iceberg/.secret/lst-consistency-8dd2dfbea73a.json");
    if (credFile.exists()) {
      try (FileInputStream creds = new FileInputStream(credFile)) {
        storage = RemoteStorageHelper.create("lst-consistency", creds).getOptions().getService();
        LOG.info("Using remote storage");
      }
    } else {
      storage = spy(LocalStorageHelper.getOptions().getService());
      // LocalStorageHelper doesn't support batch operations, so mock that here
      doAnswer(
              invoke -> {
                Iterable<BlobId> iter = invoke.getArgument(0);
                List<Boolean> answer = Lists.newArrayList();
                iter.forEach(
                        blobId -> {
                          answer.add(storage.delete(blobId));
                        });
                return answer;
              })
              .when(storage)
              .delete(any(Iterable.class));
      // LocalStorageHelper doesn't support checksums, so mock that here
      doAnswer(
              invoke -> {
                Object[] args = invoke.getArguments();
                final BlobInfo info = (BlobInfo) args[0];
                final String crc32cStr = info.getCrc32c();
                final WriteChannel out = (WriteChannel) invoke.callRealMethod();
                if (args.length == 1 || null == crc32cStr) {
                  return out;
                }
                long crc32c = Ints.fromByteArray(Base64.getDecoder().decode(crc32cStr));
                return new CRCWriteChannel(out, crc32c);
              })
              .when(storage)
              .writer(any(BlobInfo.class), any(Storage.BlobWriteOption[].class));
    }
  }

  @SuppressWarnings("unchecked")
  @BeforeEach
  public void before(TestInfo info) {
    testName = info.getTestMethod().orElseThrow(RuntimeException::new).getName();
    io = new GCSFileIO(() -> storage, new GCPProperties());
  }

  @Test
  public void newInputFile() throws IOException {
    String location = gsUri("/file.txt");
    byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);

    InputFile in = io.newInputFile(location);
    assertThat(in.exists()).isFalse();

    OutputFile out = io.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(expected));
    }

    assertThat(in.exists()).isTrue();
    byte[] actual = new byte[1024 * 1024];

    try (InputStream is = in.newStream()) {
      IOUtil.readFully(is, actual, 0, actual.length);
    }

    assertThat(expected).isEqualTo(actual);

    io.deleteFile(in);

    assertThat(io.newInputFile(location).exists()).isFalse();
  }

  @Test
  public void newOutputFileMatch() throws IOException {
    final String location = gsUri("file.txt");
    final byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);

    final OutputFile out = io.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(expected));
    }

    final InputFile in = io.newInputFile(location);
    assertThat(in.exists()).isTrue();
    final byte[] actual = new byte[1024 * 1024];

    try (InputStream is = in.newStream()) {
      IOUtil.readFully(is, actual, 0, actual.length);
    }
    assertThat(actual).isEqualTo(expected);

    OutputFile overwrite = io.newOutputFile(in);
    final byte[] overbytes = new byte[1024 * 1024];
    random.nextBytes(overbytes);
    try (OutputStream os = overwrite.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(overbytes));
    }
    // XXX !#! should fail the generation match, if resolved on this InputFile?
    try (InputStream is = in.newStream()) {
      IOUtil.readFully(is, actual, 0, actual.length);
    }
    assertThat(actual).isEqualTo(overbytes);
  }

  @Test
  public void newOutputFileMatchFail() throws IOException {
    final String location = gsUri("/file.txt");
    final byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);

    final OutputFile out = io.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(expected));
    }

    final InputFile in = io.newInputFile(location);
    assertThat(in.exists()).isTrue();
    final byte[] actual = new byte[1024 * 1024];
    try (InputStream is = in.newStream()) {
      IOUtil.readFully(is, actual, 0, actual.length);
    }
    assertThat(actual).isEqualTo(expected);

    // overwrite succeeds, because generation matches InputFile
    final OutputFile overwrite = io.newOutputFile(in);
    final byte[] overbytes = new byte[1024 * 1024];
    random.nextBytes(overbytes);
    try (OutputStream os = overwrite.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(overbytes));
    }
    // overwrite fails, object has been overwritten
    StorageException generationFailure =
        Assertions.assertThrows(
            StorageException.class,
            () -> {
              try (OutputStream os = overwrite.createOrOverwrite()) {
                IOUtil.writeFully(os, ByteBuffer.wrap(overbytes));
              }
            });
    assertThat(generationFailure.getMessage()).startsWith("Generation mismatch");
    // XXX Why does a generation mismatch return 404 (not found), and not 409 (Conflict) or 412
    // (Precondition)?
    // assertThat(generationFailure.getCode()).isEqualTo(412);
  }

  @Test
  public void testAtomicPartialWrite() throws IOException {
    final String location = gsUri("file.txt");
    final byte[] expected = new byte[1024 * 1024];
    random.nextBytes(expected);

    final OutputFile out = io.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtil.writeFully(os, ByteBuffer.wrap(expected));
    }

    final InputFile in = io.newInputFile(location);
    assertThat(in.exists()).isTrue();

    // overwrite fails, checksum does not match
    final AtomicOutputFile overwrite = io.newOutputFile(in);
    final byte[] overbytes = new byte[1024 * 1024];
    random.nextBytes(overbytes);
    final FileChecksum chk = overwrite.checksum();
    chk.update(overbytes, 0, 1024 * 1024);
    IOException hackFailure =
        Assertions.assertThrows(
            IOException.class,
            () -> {
              try (OutputStream os = overwrite.createAtomic(chk, inputFile -> {})) {
                IOUtil.writeFully(os, ByteBuffer.wrap(Arrays.copyOf(overbytes, 512 * 1024)));
              }
            });
    // HACK this is purely for the unit test, need to validate against GCP and change both to match
    assertThat(hackFailure.getMessage()).isEqualTo("CRC32C mismatch");
  }

  @Test
  public void testDelete() {
    String path = testPath("delete/path/data.dat");
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path).build());

    // There should be one blob in the bucket
    assertThat(
            StreamSupport.stream(storage.list(TEST_BUCKET, Storage.BlobListOption.prefix(testPath(""))).iterateAll().spliterator(), false)
                .count())
        .isEqualTo(1);

    io.deleteFile(gsUri(path));

    // The bucket should now be empty
    assertThat(
            StreamSupport.stream(storage.list(TEST_BUCKET, Storage.BlobListOption.prefix("delete/path")).iterateAll().spliterator(), false)
                .count())
        .isZero();
  }

  @Test
  public void testListPrefix() {
    String prefix = testPath("list/path/");
    String path1 = prefix + "data1.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path1).build());
    String path2 = prefix + "data2.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path2).build());
    String path3 = testPath("list/skip/data3.dat");
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path3).build());

    assertThat(StreamSupport.stream(io.listPrefix(gsUri("list/")).spliterator(), false).count())
        .isEqualTo(3);

    assertThat(StreamSupport.stream(io.listPrefix(gsUri("list/path/")).spliterator(), false).count())
        .isEqualTo(2);

    assertThat(StreamSupport.stream(io.listPrefix(gsUri("list/path/data1.dat")).spliterator(), false).count())
        .isEqualTo(1);
  }

  @Test
  public void testDeleteFiles() {
    String prefix = testPath("del/path/");
    String path1 = prefix + "data1.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path1).build());
    String path2 = prefix + "data2.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path2).build());
    String path3 = testPath("del/skip/data3.dat");
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path3).build());

    assertThat(StreamSupport.stream(io.listPrefix(gsUri("del/")).spliterator(), false).count())
        .isEqualTo(3);

    Iterable<String> deletes =
        () -> ImmutableList.of(gsUri("del/path/data1.dat"), gsUri("del/skip/data3.dat")).stream().iterator();
    io.deleteFiles(deletes);

    assertThat(StreamSupport.stream(io.listPrefix(gsUri("del/")).spliterator(), false).count())
        .isEqualTo(1);
  }

  @Test
  public void testDeletePrefix() {
    String prefix = testPath("del/path/");
    String path1 = prefix + "data1.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path1).build());
    String path2 = prefix + "data2.dat";
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path2).build());
    String path3 = testPath("del/skip/data3.dat");
    storage.create(BlobInfo.newBuilder(TEST_BUCKET, path3).build());

    assertThat(StreamSupport.stream(io.listPrefix(gsUri("del/")).spliterator(), false).count())
        .isEqualTo(3);

    io.deletePrefix(gsUri("del/path/"));

    assertThat(StreamSupport.stream(io.listPrefix(gsUri("del/")).spliterator(), false).count())
        .isEqualTo(1);
  }

  @Test
  public void testGCSFileIOKryoSerialization() throws IOException {
    FileIO testGCSFileIO = new GCSFileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testGCSFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.KryoHelpers.roundTripSerialize(testGCSFileIO);

    assertThat(testGCSFileIO.properties()).isEqualTo(roundTripSerializedFileIO.properties());
  }

  @Test
  public void testGCSFileIOJavaSerialization() throws IOException, ClassNotFoundException {
    FileIO testGCSFileIO = new GCSFileIO();

    // gcs fileIO should be serializable when properties are passed as immutable map
    testGCSFileIO.initialize(ImmutableMap.of("k1", "v1"));
    FileIO roundTripSerializedFileIO = TestHelpers.roundTripSerialize(testGCSFileIO);

    assertThat(testGCSFileIO.properties()).isEqualTo(roundTripSerializedFileIO.properties());
  }

  @Test
  public void testResolvingFileIOLoad() {
    ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
    resolvingFileIO.setConf(new Configuration());
    resolvingFileIO.initialize(ImmutableMap.of());
    FileIO result =
        DynMethods.builder("io")
            .hiddenImpl(ResolvingFileIO.class, String.class)
            .build(resolvingFileIO)
            .invoke("gs://foo/bar");
    assertThat(result).isInstanceOf(GCSFileIO.class);
  }

  private String gsUri(String path) {
    return String.format("gs://%s/%s", TEST_BUCKET, testPath(path));
  }

  private String testPath(String suffix) {
    return String.format("%s/%s/%s", uniqTestRun, testName, suffix);
  }
}