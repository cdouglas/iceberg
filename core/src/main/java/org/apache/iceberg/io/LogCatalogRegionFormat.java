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
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

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
  private static final String MAGIC_NUMBER = "ENDV";
  private static final int VERSION_MAJOR = 1;
  private static final int VERSION_MINOR = 0;

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
      this.format = Objects.requireNonNull(format);
      this.data = data;
    }
  }

  static void readCheckpoint(LogCatalogFormat.LogCatalogFileMut catalog, DataInputStream dis)
      throws IOException {
    final EnumMap<RegionType, Region> regions = readRegions(dis);
    catalogMetadata(catalog, regions.get(RegionType.METADATA));
    namespaceRegion(catalog, regions.get(RegionType.NS));
    nsPropRegion(catalog, regions.get(RegionType.NS_PROP));
    tableRegion(catalog, regions.get(RegionType.TABLE));
    tableEmbedRegion(catalog, regions.get(RegionType.TABLE_EMBED));
  }

  static void catalogMetadata(LogCatalogFormat.LogCatalogFileMut catalog, Region region) {
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

  private static void namespaceRegion(
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
  private static void nsPropRegion(LogCatalogFormat.LogCatalogFileMut catalog, Region region) {
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

  private static void tableRegion(LogCatalogFormat.LogCatalogFileMut catalog, Region region) {
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

  private static void tableEmbedRegion(LogCatalogFormat.LogCatalogFileMut catalog, Region region) {
    // TODO build schema for Parquet from JSON spec
    throw new UnsupportedOperationException();
  }

  private static EnumMap<RegionType, Region> readRegions(DataInputStream dis) throws IOException {
    EnumMap<RegionType, Region> regions = Maps.newEnumMap(RegionType.class);
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
      Format format = Format.from(dis.readByte());
      int length = (i == nRegions - 1) ? (dis.available()) : (endOffsets[i] - dis.available());
      byte[] data = new byte[length];
      dis.readFully(data);
      if (regions.put(type, new Region(format, data)) != null) {
        throw new IOException("Duplicate region type: " + type);
      }
    }
    return regions;
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
