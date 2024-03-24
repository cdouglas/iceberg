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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;

public class CatalogFile implements StructLike, IndexedRecord, Serializable {
  // TODO tombstone table data? Or allow ABA?
  static Types.StructType TABLE =
      Types.StructType.of(
          Types.NestedField.required(
              100,
              "table",
              Types.StructType.of(
                  Types.NestedField.required(102, "namespace", Types.StringType.get()),
                  Types.NestedField.required(103, "tableName", Types.StringType.get()),
                  Types.NestedField.optional(104, "location", Types.StringType.get()),
                  Types.NestedField.optional(105, "metadata", Types.StringType.get()))));
  static Types.NestedField TABLES =
      Types.NestedField.optional(
          106, "tables", Types.ListType.ofRequired(107, TABLE), "list of tables");
  static Schema SCHEMA = new Schema(TABLES);
  private List<StructLike> tables; // list of tables

  public CatalogFile(List<StructLike> tables) {
    this.tables = tables;
  }

  public List<StructLike> tables() {
    return tables;
  }

  static Schema schema() {
    return SCHEMA;
  }

  static class Writer {
    // Serialize the CatalogFile to the given OutputFile using Avro
    public void write(CatalogFile catalogFile, OutputFile outputFile) throws IOException {
      try (FileAppender<CatalogFile> out = Avro.write(outputFile)
          .schema(SCHEMA)
          .named("catalog")
          .build()) {
        out.add(catalogFile);
      }
    }
  }

  static class Reader {
    public CatalogFile read(InputFile file) throws IOException {
      try (CloseableIterable<CatalogFile> in = Avro.read(file)
          .project(SCHEMA)
          .reuseContainers()
          .build()) {
        CatalogFile x = in.iterator().next();
        return x;
      }
    }
  }

  static class SimpleWriter {
    // Write the class just using java's POJO serialization
    public void write(CatalogFile catalogFile, OutputFile outputFile) throws IOException, FileNotFoundException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream out = new ObjectOutputStream(baos);
      try {
        out.writeObject(catalogFile);
        PositionOutputStream posOut = outputFile.createOrOverwrite();
        posOut.write(baos.toByteArray());
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        out.close();
      }
    }
  }

  static class SimpleReader {
    // Read the class just using java's POJO serialization
    public CatalogFile read(InputFile inputFile) throws IOException, FileNotFoundException {
      SeekableInputStream fis = inputFile.newStream();
      ObjectInputStream in = new ObjectInputStream(fis);
      try {
        return (CatalogFile) in.readObject();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      } finally {
        in.close();
      }
    }
  }

  public static class TableStruct implements StructLike, IndexedRecord, Serializable {
    private String namespace;
    private String tableName;
    private String location;
    private String metadata; // This is optional based on your schema

    // Constructor
    public TableStruct(String namespace, String tableName, String location, String metadata) {
        this.namespace = namespace;
        this.tableName = tableName;
        this.location = location;
        this.metadata = metadata;
    }

    // Default constructor
    public TableStruct() {
    }

    // Implement the StructLike methods
    @Override
    public int size() {
        return 4; // Corresponding to the number of fields: namespace, tableName, location, and metadata
    }

    @Override
    public Object get(int i) {
      int pos = i;
      switch (pos) {
          case 0: return namespace;
          case 1: return tableName;
          case 2: return location;
          case 3: return metadata;
          default: throw new IllegalArgumentException("Invalid position: " + pos);
      }
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      Object value = get(pos);
      if (value == null) {
          return null;
      }
      return javaClass.cast(value);
    }

    @Override
    public void set(int pos, Object value) {
      switch (pos) {
        case 0: this.namespace = (String) value.toString(); break;
        case 1: this.tableName = (String) value.toString(); break;
        case 2: this.location = (String) value.toString(); break;
        case 3: this.metadata = (String) value.toString(); break;
        default: throw new IllegalArgumentException("Invalid position: " + pos);
      }
      return; // Return the current instance for method chaining
    }

    // Getters and setters for the fields (if necessary for your application logic)

    @Override
    public int hashCode() {
        return Objects.hash(namespace, tableName, location, metadata);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TableStruct other = (TableStruct) obj;
        return Objects.equals(namespace, other.namespace)
            && Objects.equals(tableName, other.tableName)
            && Objects.equals(location, other.location)
            && Objects.equals(metadata, other.metadata);
    }

    @Override
    public org.apache.avro.Schema getSchema() {
      return AvroSchemaUtil.convert(TABLE, "table");
    }

    @Override
    public void put(int i, Object v) {
      set(i, v);
    }
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    return AvroSchemaUtil.convert(SCHEMA, "tables");
  }

  @Override
  public Object get(int i) {
    // Since the schema only defines one top-level field ("tables"), we only need to handle that one case
    if (i != 0) {
      throw new IndexOutOfBoundsException("CatalogFile has only one field.");
    }
    return tables; // The only field this example class handles
  }

  @Override
  public int size() {
    return SCHEMA.columns().size();
  }

  @Override
  public void put(int i, Object v) {
    set(i, v);
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(get(pos));
  }

  @Override
  public <T> void set(int pos, T value) {
    switch (pos) {
      default:
        put(pos, value);
    }
  }
}
