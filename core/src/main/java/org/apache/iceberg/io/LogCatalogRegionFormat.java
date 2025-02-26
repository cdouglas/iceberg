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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

// TODO what the hell were you doing? This is a mess.

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
final class LogCatalogRegionFormat {
  private static final byte[] MAGIC_NUMBER = "ENDV".getBytes(StandardCharsets.US_ASCII);
  private static final int VERSION_MAJOR = 1;
  private static final int VERSION_MINOR = 0;

  static class LogCatalogFile extends CatalogFile {
    final int nextNsid;
    final int nextTblid;

    private final Map<Namespace, Integer> nsids;
    private final Map<Integer, Integer> nsVersion;
    private final Map<Integer, Namespace> nsLookup;

    private final Map<Integer, Map<String, String>> nsProperties;

    private final Map<TableIdentifier, Integer> tblIds;
    private final Map<Integer, Integer> tblVersion;
    private final Map<Integer, String> tblLocations;

    // empty LogCatalogFile
    LogCatalogFile(InputFile location) {
      super(location);
      this.nextNsid = 1;
      this.nextTblid = 1;
      this.nsids = Maps.newHashMap();
      this.nsVersion = Maps.newHashMap();
      this.nsProperties = Maps.newHashMap();
      this.tblIds = Maps.newHashMap();
      this.tblVersion = Maps.newHashMap();
      this.tblLocations = Maps.newHashMap();
      this.nsLookup = Maps.newHashMap();
    }

    LogCatalogFile(
        InputFile location,
        UUID catalogUUID,
        int nextNsid,
        int nextTblid,
        Map<Namespace, Integer> nsids,
        Map<Integer, Integer> nsVersion,
        Map<Integer, Map<String, String>> nsProperties,
        Map<TableIdentifier, Integer> tblIds,
        Map<Integer, Integer> tblVersion,
        Map<Integer, String> tblLocations) {
      super(catalogUUID, location);
      this.nextNsid = nextNsid;
      this.nextTblid = nextTblid;
      this.nsids = nsids;
      this.nsVersion = nsVersion;
      this.nsProperties = nsProperties;
      this.tblIds = tblIds;
      this.tblVersion = tblVersion;
      this.tblLocations = tblLocations;
      this.nsLookup =
          nsids.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    @Override
    public String location(TableIdentifier table) {
      final Integer tblId = tblIds.get(table);
      if (tblId == null) {
        return null;
      }
      return tblLocations.get(tblId);
    }

    @Override
    public Set<Namespace> namespaces() {
      return Collections.unmodifiableSet(nsids.keySet());
    }

    @Override
    public boolean containsNamespace(Namespace namespace) {
      return nsids.containsKey(namespace);
    }

    @Override
    public Map<String, String> namespaceProperties(Namespace namespace) {
      final Integer nsid = nsids.get(namespace);
      if (nsid == null) {
        return null;
      }
      return Collections.unmodifiableMap(nsProperties.get(nsid));
    }

    @Override
    public List<TableIdentifier> tables() {
      return Lists.newArrayList(tblIds.keySet().iterator());
    }

    @Override
    Map<Namespace, Map<String, String>> namespaceProperties() {
      return nsids.entrySet().stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey,
                  e -> nsProperties.getOrDefault(e.getValue(), Collections.emptyMap())));
    }

    @Override
    Map<TableIdentifier, String> locations() {
      return tblIds.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> tblLocations.get(e.getValue())));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LogCatalogFile that = (LogCatalogFile) o;
      return nextNsid == that.nextNsid
          && nextTblid == that.nextTblid
          && Objects.equals(uuid(), that.uuid())
          && Objects.equals(nsids, that.nsids)
          && Objects.equals(nsVersion, that.nsVersion)
          && Objects.equals(nsProperties, that.nsProperties)
          && Objects.equals(tblIds, that.tblIds)
          && Objects.equals(tblVersion, that.tblVersion)
          && Objects.equals(tblLocations, that.tblLocations);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          uuid(), nextNsid, nextTblid, nsids, nsVersion, nsProperties, tblIds, tblVersion, tblLocations);
    }

    @Override
    public String toString() {
      return "LogCatalogFile{"
          + "catalogUUID="
          + uuid()
          + ", nextNsid="
          + nextNsid
          + ", nextTblid="
          + nextTblid
          + ", nsids="
          + nsids
          + ", nsVersion="
          + nsVersion
          + ", nsProperties="
          + nsProperties
          + ", tblIds="
          + tblIds
          + ", tblVersion="
          + tblVersion
          + ", tblLocations="
          + tblLocations
          + '}';
    }

    Iterable<NamespaceEntry> namespaceEntries() {
      return () ->
          new Iterator<NamespaceEntry>() {
            // sort by nsid; sufficient for parentId, since namespaces never move and are created in
            // order
            private final Iterator<Map.Entry<Namespace, Integer>> iter =
                nsids.entrySet().stream()
                    .sorted(Comparator.comparingInt(Map.Entry::getValue))
                    .iterator();

            @Override
            public boolean hasNext() {
              return iter.hasNext();
            }

            @Override
            public NamespaceEntry next() {
              Map.Entry<Namespace, Integer> entry = iter.next();
              final Namespace ns = entry.getKey();
              final int levels = ns.length();
              final Namespace parent =
                  levels > 1
                      ? Namespace.of(Arrays.copyOfRange(ns.levels(), 0, levels - 1))
                      : Namespace.empty();
              final int nsid = entry.getValue();
              return new NamespaceEntry(
                  nsid,
                  nsVersion.get(nsid),
                  nsids.get(parent),
                  0 == levels ? "" : ns.level(levels - 1));
            }
          };
    }

    Iterable<NamespacePropertyEntry> namespacePropertyEntries() {
      return () ->
          new Iterator<NamespacePropertyEntry>() {
            private final Iterator<Map.Entry<Integer, Map<String, String>>> iter =
                nsProperties.entrySet().iterator();
            private Integer currentKey = null;
            private Iterator<Map.Entry<String, String>> currentIter = null;

            @Override
            public boolean hasNext() {
              if (currentIter == null || !currentIter.hasNext()) {
                while (iter.hasNext()) {
                  Map.Entry<Integer, Map<String, String>> e = iter.next();
                  currentKey = e.getKey();
                  currentIter = e.getValue().entrySet().iterator();
                  if (currentIter.hasNext()) {
                    return true;
                  }
                }
                return false;
              }
              return true;
            }

            @Override
            public NamespacePropertyEntry next() {
              Map.Entry<String, String> entry = currentIter.next();
              return new NamespacePropertyEntry(currentKey, entry.getKey(), entry.getValue());
            }
          };
    }

    Iterable<TableEntry> tableEntries() {
      return () ->
          new Iterator<TableEntry>() {
            private final Iterator<Map.Entry<TableIdentifier, Integer>> iter =
                tblIds.entrySet().iterator();

            @Override
            public boolean hasNext() {
              return iter.hasNext();
            }

            @Override
            public TableEntry next() {
              Map.Entry<TableIdentifier, Integer> entry = iter.next();
              final TableIdentifier tblId = entry.getKey();
              final int tblid = entry.getValue();
              return new TableEntry(
                  tblid,
                  tblVersion.get(tblid),
                  nsids.get(tblId.namespace()),
                  tblId.name(),
                  tblLocations.get(tblid));
            }
          };
    }
  }

  enum RegionType {
    NS(1),
    NS_PROP(2),
    TABLE(3),
    TABLE_EMBED(4),
    METADATA(5);
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

  // TODO really... lame way to do this, but whatever.
  enum Format {
    LENGTH(1), // rename to LOG and ignore validation for checkpoint replay
    JSON(2),
    PARQUET(3);
    private final int fmtid;

    Format(int format) {
      this.fmtid = format;
    }

    static Format from(int format) {
      for (Format f : Format.values()) {
        if (f.fmtid == format) {
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
      this.format = Objects.requireNonNull(format);
      this.data = data;
    }
  }

  static void readCheckpoint(LogCatalogFormat.LogCatalogFileMut catalog, InputStream stream)
      throws IOException {
    // TODO WHY doesn't this just use the log format?
    DataInputStream dis = new DataInputStream(stream);
    final EnumMap<RegionType, Region> regions = readRegions(dis);
    readMetadataRegion(catalog, regions.get(RegionType.METADATA));
    readNamespaceRegion(catalog, regions.get(RegionType.NS));
    readNsPropRegion(catalog, regions.get(RegionType.NS_PROP));
    readTableRegion(catalog, regions.get(RegionType.TABLE));
    readTableEmbedRegion(catalog, regions.get(RegionType.TABLE_EMBED));
  }

  private static void readMetadataRegion(
      LogCatalogFormat.LogCatalogFileMut catalog, Region region) {
    if (null == region) {
      throw new IllegalStateException("Metadata region is required");
    }
    if (region.format == Format.LENGTH) {
      try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(region.data))) {
        long msb = dis.readLong();
        long lsb = dis.readLong();
        final UUID uuid = new UUID(msb, lsb);
        final int nextNsid = dis.readInt();
        final int nextTblid = dis.readInt();
        catalog.setGlobals(uuid, nextNsid, nextTblid);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    } else {
      throw new UnsupportedOperationException(region.format + " unsupported");
    }
  }

  private static void readNamespaceRegion(
      LogCatalogFormat.LogCatalogFileMut catalog, final Region region) {
    // strict versioning of namespace properties requires at least the empty namespace be present
    Preconditions.checkNotNull(region, "Missing namespace region");

    // build (nsid, version) -> Namespace
    final Iterable<NamespaceEntry> nsEntries;
    switch (region.format) {
      case LENGTH:
        nsEntries = () -> new LengthNamespaceIterator(region.data);
        break;
      case JSON:
        nsEntries = () -> new JsonNamespaceIterator(region.data);
        break;
      default:
        throw new UnsupportedOperationException(
            region.format + " unsupported for namespace region");
    }
    for (NamespaceEntry e : nsEntries) {
      catalog.addNamespaceInternal(e.name, e.parentId, e.nsid, e.version);
    }
  }

  // updating properties increments the ns version, so confluent updates to properties are treated
  // as conflicts
  private static void readNsPropRegion(LogCatalogFormat.LogCatalogFileMut catalog, Region region) {
    if (region == null) {
      return;
    }
    final Iterable<NamespacePropertyEntry> nsPropEntries;
    switch (region.format) {
      case LENGTH:
        nsPropEntries = () -> new LengthNamespacePropertyIterator(region.data);
        break;
      case JSON:
        nsPropEntries = () -> new JsonNamespacePropertyIterator(region.data);
        break;
      case PARQUET:
      default:
        throw new UnsupportedOperationException(region.format + " unsupported for property region");
    }
    for (NamespacePropertyEntry nsPropEntry : nsPropEntries) {
      catalog.addNamespacePropertyInternal(nsPropEntry.nsid, nsPropEntry.key, nsPropEntry.value);
    }
  }

  private static void readTableRegion(LogCatalogFormat.LogCatalogFileMut catalog, Region region) {
    // TODO
    if (null == region) {
      return;
    }
    final Iterable<TableEntry> tableEntries;
    switch (region.format) {
      case LENGTH:
        tableEntries = () -> new LengthTableIterator(region.data);
        break;
      case JSON:
        tableEntries = () -> new JsonTableIterator(region.data);
        break;
      case PARQUET:
      default:
        throw new UnsupportedOperationException(region.format + " unsupported for table region");
    }
    for (TableEntry e : tableEntries) {
      catalog.addTableInternal(e.tblId, e.nsid, e.version, e.name, e.location);
    }
  }

  private static void readTableEmbedRegion(
      LogCatalogFormat.LogCatalogFileMut catalog, Region region) {
    // TODO build schema for Parquet from JSON spec
    if (region != null) {
      throw new UnsupportedOperationException();
    }
  }

  private static EnumMap<RegionType, Region> readRegions(DataInputStream dis) throws IOException {
    EnumMap<RegionType, Region> regions = Maps.newEnumMap(RegionType.class);
    byte[] magic = new byte[4];
    dis.readFully(magic);
    if (!Arrays.equals(MAGIC_NUMBER, magic)) {
      throw new IOException("Invalid magic number");
    }

    int versionMajor = dis.readUnsignedShort();
    int versionMinor = dis.readUnsignedShort();
    if (versionMajor != VERSION_MAJOR || versionMinor != VERSION_MINOR) {
      throw new IOException("Unsupported version");
    }

    int nRegions = dis.readUnsignedShort();
    int[] endOffsets = new int[nRegions];
    for (int i = 0; i < nRegions; i++) {
      endOffsets[i] = dis.readInt();
    }

    int previousOffset = headerLen(nRegions);
    for (int i = 0; i < nRegions; i++) {
      RegionType type = RegionType.from(dis.readByte());
      Format format = Format.from(dis.readByte());
      int length = endOffsets[i] - previousOffset;
      byte[] data = new byte[length];
      dis.readFully(data);
      if (regions.put(type, new Region(format, data)) != null) {
        throw new IOException("Duplicate region type: " + type);
      }
    }
    return regions;
  }

  private static int headerLen(int nRegions) {
    return MAGIC_NUMBER.length + 2 + 2 + 2 + (nRegions * 4);
  }

  @VisibleForTesting
  static Supplier<InputStream> writeCheckpoint(
      LogCatalogFile catalog, EnumMap<RegionType, Format> formats) {
    final EnumMap<RegionType, Region> regions = Maps.newEnumMap(RegionType.class);
    // TODO multi-part upload, don't buffer embedded tables into memory
    for (Map.Entry<RegionType, Format> entry : formats.entrySet()) {
      final RegionType type = entry.getKey();
      switch (type) {
        case NS:
          regions.put(type, writeNamespaceRegion(catalog, entry.getValue()));
          break;
        case NS_PROP:
          regions.put(type, writeNsPropRegion(catalog, entry.getValue()));
          break;
        case TABLE:
          regions.put(type, writeTableRegion(catalog, entry.getValue()));
          break;
        case TABLE_EMBED:
          throw new UnsupportedOperationException("Unsupported");
        case METADATA:
          throw new UnsupportedOperationException("metadata format not configurable");
        default:
          throw new UnsupportedOperationException("Unknown region type: " + type);
      }
    }
    regions.put(RegionType.METADATA, writeMetadataRegion(catalog));
    return writeRegions(regions);
  }

  private static Region writeMetadataRegion(LogCatalogFile catalog) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {
      UUID catalogUUID = catalog.uuid();
      dos.writeLong(catalogUUID.getMostSignificantBits());
      dos.writeLong(catalogUUID.getLeastSignificantBits());
      dos.writeInt(catalog.nextNsid);
      dos.writeInt(catalog.nextTblid);
      return new Region(Format.LENGTH, bos.toByteArray());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Region writeNamespaceRegion(LogCatalogFile catalog, Format format) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {
      switch (format) {
        case LENGTH:
          dos.writeInt(catalog.nsids.size());
          for (NamespaceEntry e : catalog.namespaceEntries()) {
            dos.writeInt(e.nsid);
            dos.writeInt(e.version);
            dos.writeInt(e.parentId);
            dos.writeUTF(e.name);
          }
          break;
        default:
          throw new UnsupportedOperationException(format + " unsupported for namespace region");
      }
      // TODO so many unnecessary buffer copies, ffs
      return new Region(format, bos.toByteArray());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Region writeNsPropRegion(LogCatalogFile catalog, Format format) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {
      switch (format) {
        case LENGTH:
          dos.writeInt(catalog.nsProperties.values().stream().mapToInt(Map::size).sum());
          for (NamespacePropertyEntry e : catalog.namespacePropertyEntries()) {
            dos.writeInt(e.nsid);
            dos.writeUTF(e.key);
            dos.writeUTF(e.value);
          }
          break;
        default:
          throw new UnsupportedOperationException(
              format + " unsupported for namespace property region");
      }
      return new Region(format, bos.toByteArray());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Region writeTableRegion(LogCatalogFile catalog, Format format) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos)) {
      switch (format) {
        case LENGTH:
          dos.writeInt(catalog.tblIds.size());
          for (TableEntry e : catalog.tableEntries()) {
            dos.writeInt(e.tblId);
            dos.writeInt(e.version);
            dos.writeInt(e.nsid);
            dos.writeUTF(e.name);
            dos.writeUTF(e.location);
          }
          break;
        default:
          throw new UnsupportedOperationException(format + " unsupported for table region");
      }
      return new Region(format, bos.toByteArray());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // TODO aaaaaaand another buffer copy. You shame yourself.
  private static Supplier<InputStream> writeRegions(EnumMap<RegionType, Region> regions) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos)) {
      // write METADATA region
      // FFS why are you making this so convoluted?
      out.write(MAGIC_NUMBER);
      out.writeShort(VERSION_MAJOR);
      out.writeShort(VERSION_MINOR);
      final int nRegions = regions.size();
      out.writeShort(nRegions);
      // MAGIC(4) MAJOR(2) MINOR(2) NREGIONS(2) ENDOFFSETS(4*NREGIONS)
      final int headerOffset = headerLen(nRegions);
      for (Region region : regions.values()) {
        out.writeInt(headerOffset + region.data.length);
      }
      for (Map.Entry<RegionType, Region> re : regions.entrySet()) {
        out.writeByte(re.getKey().type);
        final Region region = re.getValue();
        out.writeByte(region.format.fmtid);
        out.write(region.data);
      }
      return () -> new ByteArrayInputStream(bos.toByteArray());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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

    public LengthNamespaceIterator(byte[] data) {
      try {
        this.dis = new DataInputStream(new ByteArrayInputStream(data));
        this.nNamespaces = dis.readInt();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
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

    public JsonNamespaceIterator(byte[] jsonData) {
      try {
        this.objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(jsonData);
        List<JsonNode> jsonNodes = new ArrayList<>();
        rootNode.elements().forEachRemaining(jsonNodes::add);
        this.jsonIterator = jsonNodes.iterator();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
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
          jsonNode.get("name").asText());
    }
  }

  static class NamespacePropertyEntry {
    final int nsid;
    final String key;
    final String value;

    NamespacePropertyEntry(int nsid, String key, String value) {
      this.nsid = nsid;
      this.key = Objects.requireNonNull(key);
      this.value = Objects.requireNonNull(value);
    }
  }

  static class LengthNamespacePropertyIterator implements Iterator<NamespacePropertyEntry> {
    private final DataInputStream dis;
    private final int nProps;
    private int currentIndex = 0;

    public LengthNamespacePropertyIterator(byte[] data) {
      try {
        this.dis = new DataInputStream(new ByteArrayInputStream(data));
        this.nProps = dis.readInt();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public boolean hasNext() {
      return currentIndex < nProps;
    }

    @Override
    public NamespacePropertyEntry next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        int nsid = dis.readInt();
        String key = dis.readUTF();
        String value = dis.readUTF();
        currentIndex++;
        return new NamespacePropertyEntry(nsid, key, value);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  static class JsonNamespacePropertyIterator implements Iterator<NamespacePropertyEntry> {
    private final Iterator<JsonNode> jsonIterator;
    private final ObjectMapper objectMapper;

    public JsonNamespacePropertyIterator(byte[] jsonData) {
      try {
        this.objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(jsonData);
        List<JsonNode> jsonNodes = new ArrayList<>();
        rootNode.elements().forEachRemaining(jsonNodes::add);
        this.jsonIterator = jsonNodes.iterator();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public boolean hasNext() {
      return jsonIterator.hasNext();
    }

    @Override
    public NamespacePropertyEntry next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      JsonNode jsonNode = jsonIterator.next();
      return new NamespacePropertyEntry(
          jsonNode.get("nsid").asInt(),
          jsonNode.get("key").asText(),
          jsonNode.get("value").asText());
    }
  }

  static class TableEntry {
    final int tblId;
    final int version;
    final int nsid;
    final String name;
    final String location;

    TableEntry(int tblId, int version, int nsid, String name, String location) {
      this.tblId = tblId;
      this.version = version;
      this.nsid = nsid;
      this.name = name;
      this.location = location;
    }
  }

  static class LengthTableIterator implements Iterator<TableEntry> {
    private final DataInputStream dis;
    private final int nTables;
    private int currentIndex = 0;

    public LengthTableIterator(byte[] data) {
      try {
        this.dis = new DataInputStream(new ByteArrayInputStream(data));
        this.nTables = dis.readInt();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public boolean hasNext() {
      return currentIndex < nTables;
    }

    @Override
    public TableEntry next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        int tblId = dis.readInt();
        int version = dis.readInt();
        int nsid = dis.readInt();
        String name = dis.readUTF();
        String location = dis.readUTF();
        currentIndex++;
        return new TableEntry(tblId, version, nsid, name, location);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  static class JsonTableIterator implements Iterator<TableEntry> {
    private final Iterator<JsonNode> jsonIterator;
    private final ObjectMapper objectMapper;

    public JsonTableIterator(byte[] jsonData) {
      try {
        this.objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(jsonData);
        List<JsonNode> jsonNodes = new ArrayList<>();
        rootNode.elements().forEachRemaining(jsonNodes::add);
        this.jsonIterator = jsonNodes.iterator();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public boolean hasNext() {
      return jsonIterator.hasNext();
    }

    @Override
    public TableEntry next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      JsonNode jsonNode = jsonIterator.next();
      return new TableEntry(
          jsonNode.get("tblId").asInt(),
          jsonNode.get("version").asInt(),
          jsonNode.get("nsid").asInt(),
          jsonNode.get("name").asText(),
          jsonNode.get("location").asText());
    }
  }
}
