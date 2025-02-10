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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class LogCatalogFormat extends CatalogFormat {
  private final CASCatalogFormat casFormat = new CASCatalogFormat();

  @Override
  public CatalogFile.Mut empty(InputFile input) {
    return new LogCatalogFileMut(input);
  }

  @Override
  public CatalogFile read(SupportsAtomicOperations fileIO, InputFile catalogLocation) {
    // intentionally drop metadata cached on InputFile
    InputFile refresh = fileIO.newInputFile(catalogLocation.location());
    final Map<TableIdentifier, CatalogFile.TableInfo> fqti = Maps.newHashMap();
    final Map<Namespace, Map<String, String>> namespaces = Maps.newHashMap();
    final long fileLength = refresh.getLength();
    try (SeekableInputStream in = refresh.newStream();
         DataInputStream din = new DataInputStream(in)) {
      LogCatalogFileMut chk = readCheckpoint(din);
      long logLength = fileLength - in.getPos();
      while (logLength > 0) {
        din.readUnsignedByte()
      }
      return new CatalogFile(new UUID(msb, lsb), seqno, namespaces, fqti, catalogLocation);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static LogCatalogFileMut readCheckpoint(DataInputStream din) {
    // read checkpoint
    // format
    // UUID + version
    // namespaces
    // namespace(_id_, _ver_, parent, name)
    // ns_prop(_id_, _ver_, key, value)
    // table(_id_, _ver_, nsid, name, locid)
    // location(locid, bytes or location)

    return null;
  }

  @Override
  public CatalogFile.Mut from(CatalogFile other) {
  }

  // Log format
  // <len><op><payload>
  enum Action {
    CREATE_TABLE(1),     // <tbl_id> <nsid> <name> <location>
    UPDATE_TABLE(2),     // <tbl_id> <version> <location>
    DROP_TABLE(3),       // <tbl_id> <version>
    CREATE_NAMESPACE(4), //
    UPDATE_NAMESPACE(5),
    DROP_NAMESPACE(6)
    ;

    private int opcode;
    Action(int opcode) {
      this.opcode = opcode;
    }
    abstract void apply(byte[] data);
  }


  static class LogCatalogFileMut extends CatalogFile.Mut {

      LogCatalogFileMut(InputFile input) {
        super(input);
      }

      void logEntry(LogEntry entry) {

      }

    // storage
    // prepare: configure constraints
    // writeAtomic: given constraints, write data

    // refresh(tok: retry constraints)
    //   re-read properties of InputFile from storage
    //   tok::check(InputFile)
    //     if length > max, then throw
    //     if attempts > max, then throw
    //     if latency > max, then throw

    // writeAtomic (CAS)
    //    attempt write(InputStream) with etag
    //    case (failure)
    //      bad etag:
    //        throw (need to merge)

    // writeAtomic (append) TODO need refresh
    //   LOOP A
    //     offset <- tok::getOffset
    //     attempt write(InputStream)@offset with etag
    //     case (failure)
    //       bad offset:
    //         refresh(tok::getRetryConstraints);
    //         goto A
    //       bad etag:
    //         throw (need to merge)
    //       I/O error:
    //         rethrow

    // logappend(data: bytes[])
    //   read object, including log
    //   LOOP A
    //     if log contains a compaction directive
    //       attempt CAS with merged state (include compaction UUID)
    //       CAS failed:
    //         read object, including log
    //         GOTO A
    //
    //   LOOP B

    // create update log entry
    // LOOP A
    //   read object
    //   if length > max AND log does NOT contain a compaction directive
    //     logAppend(compaction)
    //
    //
    //   attempt to append to original offset; if successful

    @Override
    public CatalogFile commit(SupportsAtomicOperations fileIO) {
      try {
        CatalogFile catalog = merge();
        final AtomicOutputFile<CAS> outputFile = fileIO.newOutputFile(original.location());
        try {
          byte[] ffs = asBytes(catalog);
          try (ByteArrayInputStream serBytes = new ByteArrayInputStream(asBytes(catalog))) {
            serBytes.mark(ffs.length); // readAheadLimit ignored, but whatever
            CAS token = outputFile.prepare(() -> serBytes, AtomicOutputFile.Strategy.APPEND);
            serBytes.reset();
            InputFile newCatalog = outputFile.writeAtomic(token, () -> serBytes);
            return new CatalogFile(
                    catalog.uuid(),
                    catalog.seqno(),
                    catalog.namespaceProperties(),
                    catalog.tableMetadata(),
                    newCatalog);
          }
        } catch (IOException e) {
          throw new CommitFailedException(e, "Failed to commit catalog file");
        }
      } catch (SupportsAtomicOperations.CASException e) {
        throw new CommitFailedException(e, "Cannot commit");
      }
      return null;
    }
  }

  // ENDV<ver_major><ver_minor><nregions>[<region_end>]*
  // [<region_type><region_format><region_data>]*
  //
  // CREATE TABLE namespaces (
  //   nsid INT,
  //   version INT,
  //   parentId INT,
  //   name VARCHAR(255) NOT NULL,
  //   PRIMARY KEY (nsid, version),
  //   FOREIGN KEY (parentId) REFERENCES namespaces(nsid)
  // );
  //
  // CREATE TABLE ns_prop (
  //   nsid INT,
  //   key VARCHAR(255) NOT NULL,
  //   value VARCHAR(1024) NOT NULL
  // );
  //
  // CREATE TABLE tables (
  //   tbl_id INT,
  //   version INT,
  //   nsid INT,
  //   name VARCHAR(255) NOT NULL,
  //   location VARCHAR(1024 NOT NULL),
  //   PRIMARY KEY (tbl_id, version),
  //   FOREIGN KEY (nsid) REFERENCES namespaces(nsid)
  // );
  //
  // *TABLE_EMBED*
  static final class RegionFormat {
    private static final String MAGIC_NUMBER = "ENDV";
    private static final int VERSION_MAJOR = 1;
    private static final int VERSION_MINOR = 0;
    enum RegionType {
      NS(1),
      NS_PROP(2),
      TABLE(3),
      TABLE_EMBED(4);
      private final byte type;
      RegionType(int type) {
        this.type = (byte) type;
      }
      static RegionType from(byte type) {
        for (RegionType rt : RegionType.values()) {
          if (rt.type == type) {
            return rt;
          }
        }
        throw new IllegalArgumentException("Unknown region type: " + type);
      }
    }
    enum Format { // extend w/ versions... more work to do be done in a prod implementation
      LENGTH(1),
      JSON(2),
      PARQUET(3);
      private final int format;
      Format(int format) {
        this.format = format;
      }
      static Format from(int format) {
        for (Format f : Format.values()) {
          if (f.format == format) {
            return f;
          }
        }
        throw new IllegalArgumentException("Unknown format: " + format);
      }
    }
    static class Region {
      final Format format;
      final byte[] data;
      Region(Format format, byte[] data) {
        this.format = format;
        this.data = data;
      }
    }
    static class NSinfo {
      final int nsid;
      final int version;
      NSinfo(int nsid, int version) {
        this.nsid = nsid;
        this.version = version;
      }
    }
    static CatalogFile from(InputFile catalogLocation, SupportsAtomicOperations fileIO) throws IOException {
      InputFile refresh = fileIO.newInputFile(catalogLocation.location()); // discard metadata
      try (SeekableInputStream in = refresh.newStream();
           DataInputStream dis = new DataInputStream(in)) {
        final EnumMap<RegionType,Region> regions = readRegions(dis);
        // nsid -> ns properties
        Map<Integer, Map<String, String>> nsProperties = nsPropRegion(regions.get(RegionType.NS_PROP));
        // Namespace -> (nsid, version)
        Map<Integer, Namespace> nsids = namespaceRegion(regions.get(RegionType.NS);
        // Map<Namespace, Map<String, String>> namespaces = namespaceRegion(regions.get(RegionType.NS), nsProperties);
      }

      return null;
    }

    // updating properties increments the ns version, so confluent updates to properties are treated as conflicts
    private static Map<Integer, Map<String,String>> nsPropRegion(Region region) {
        if (region == null) {
          return Maps.newHashMap();
        }
        switch (region.format) {
          case LENGTH:
            Map<Integer, Map<String,String>> nsProperties = Maps.newHashMap();
            try (ByteArrayInputStream bais = new ByteArrayInputStream(region.data);
                 DataInputStream dis = new DataInputStream(bais)) {
              int nProps = dis.readInt();
              for (int i = 0; i < nProps; i++) {
                int nsid = dis.readInt();
                int nPropsForNs = dis.readInt();
                Map<String,String> props = Maps.newHashMap();
                for (int j = 0; j < nPropsForNs; j++) {
                  String key = dis.readUTF();
                  String value = dis.readUTF();
                  props.put(key, value);
                }
                nsProperties.put(nsid, props);
              }
              return nsProperties;
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          case JSON:
          case PARQUET:
          default:
            throw new UnsupportedOperationException(region.format + " unsupported");
        }
    }

    private static Map<Namespace, NSinfo> namespaceRegion(Region region) {
      // strict versioning of namespace properties requires at least the empty namespace be present
      Preconditions.checkNotNull(region, "Missing namespace region");

      Map<Namespace, NSinfo> namespaces = Maps.newHashMap();

      // build (nsid, version) -> Namespace
      final Iterable<NamespaceEntry> nsEntries;
      switch (region.format) {
        case LENGTH:
          nsEntries = new LengthNamespaceIterator(region.data);
          break;
        case JSON:
          nsEntries = new JsonNamespaceIterator(region.data);
          break;
        case PARQUET:
        default:
          throw new UnsupportedOperationException();
      }

        Map<Integer, String> names = Maps.newHashMap();
        Map<Integer, Integer> parents = Maps.newHashMap();
        Map<Integer, Integer> versions = Maps.newHashMap();
        for (NamespaceEntry e : nsEntries) {
        if (e.nsid == 0) {
          Preconditions.checkArgument(e.parentId == 0, "Invalid parent id for root namespace");
          Preconditions.checkArgument(e.name.isEmpty(), "Invalid name for root namespace");
          if (null != namespaces.put(Namespace.empty(), new NSinfo(0, e.version))) {
            throw new IllegalStateException("Duplicate root namespace");
          }
          continue;
        }

        Preconditions.checkArgument(!e.name.isEmpty(), "Invalid empty name for non-root namespace");

        ArrayDeque<String> qualName = new ArrayDeque<>();
        while (e.parentId != 0) { // TODO set a max depth
          if (!parents.containsKey(e.parentId)) {
            throw new IllegalArgumentException("Unknown parent namespace: " + e.parentId);
          }
          qualName.addFirst(names.get(e.parentId));
          e.parentId = parents.get(e.parentId);
        }
        namespaces.put(Namespace.of(qualName.toArray(new String[0])), new NSinfo(e.nsid, e.version));
      }
    }

    private static EnumMap<RegionType,Region> readRegions(DataInputStream dis) throws IOException {
      EnumMap<RegionType,Region> regions = Maps.newEnumMap(RegionType.class);
      byte[] magic = new byte[4];
      dis.readFully(magic);
      if (!MAGIC_NUMBER.equals(new String(magic, StandardCharsets.UTF_8))) {
          throw new IOException("Invalid magic number");
      }

      int versionMajor = dis.readUnsignedShort();
      int versionMinor = dis.readUnsignedShort();
      if (versionMajor != VERSION_MAJOR || versionMinor != VERSION_MINOR) {
          throw new IOException("Unsupported version");
      }

      int nRegions = dis.readInt();
      int[] endOffsets = new int[nRegions];
      for (int i = 0; i < nRegions; i++) {
          endOffsets[i] = dis.readInt();
      }

      for (int i = 0; i < nRegions; i++) {
          RegionType type = RegionType.from(dis.readByte());
          Format format = Format.from(dis.readInt());
          int length = (i == nRegions - 1) ? (int) (dis.available()) : (endOffsets[i] - dis.available());
          byte[] data = new byte[length];
          dis.readFully(data);
          if (regions.put(type, new Region(format, data)) != null) {
              throw new IOException("Duplicate region type: " + type);
          }
      }
      return regions;
    }
  }

  static class NamespaceEntry {
    final int nsid;
    final int version;
    final int parentId;
    final String name;
    NamespaceEntry(int nsid, int version, int parentId, String name) {
      this.nsid = nsid;
      this.version = version;
      this.parentId = parentId;
      this.name = name;
    }
  }

  static class LengthNamespaceIterator implements Iterator<NamespaceEntry> {
      private final DataInputStream dis;
      private final int nNamespaces;
      private int currentIndex = 0;

      public LengthNamespaceIterator(byte[] data) throws IOException {
          this.dis = new DataInputStream(new ByteArrayInputStream(data));
          this.nNamespaces = dis.readInt();
      }

      @Override
      public boolean hasNext() {
          return currentIndex < nNamespaces;
      }

      @Override
      public NamespaceEntry next() {
          if (!hasNext()) {
              throw new NoSuchElementException();
          }
          try {
              int nsid = dis.readInt();
              int version = dis.readInt();
              int parentId = dis.readInt();
              String name = dis.readUTF();
              currentIndex++;
              return new NamespaceEntry(nsid, version, parentId, name);
          } catch (IOException e) {
              throw new UncheckedIOException(e);
          }
      }
  }

  static class JsonNamespaceIterator implements Iterator<NamespaceEntry> {
      private final Iterator<JsonNode> jsonIterator;
      private final ObjectMapper objectMapper;

      public JsonNamespaceIterator(List<String> jsonList) {
          this.objectMapper = new ObjectMapper();
          this.jsonIterator = jsonList.stream().map(json -> {
              try {
                  return objectMapper.readTree(json);
              } catch (IOException e) {
                  throw new UncheckedIOException(e);
              }
          }).iterator();
      }

      @Override
      public boolean hasNext() {
          return jsonIterator.hasNext();
      }

      @Override
      public NamespaceEntry next() {
          if (!hasNext()) {
              throw new NoSuchElementException();
          }
          JsonNode jsonNode = jsonIterator.next();
          return new NamespaceEntry(
              jsonNode.get("nsid").asInt(),
              jsonNode.get("version").asInt(),
              jsonNode.get("parentId").asInt(),
              jsonNode.get("name").asText()
          );
      }
  }
}
