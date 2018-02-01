/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.test.rowSet;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.ColumnMetadata;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.TupleSchema.MapColumnMetadata;

/**
 * Builder of a row set schema expressed as a list of materialized
 * fields. Optimized for use when creating schemas by hand in tests.
 * <p>
 * Example usage to create the following schema: <br>
 * <tt>(c: INT, a: MAP(b: VARCHAR, d: INT, e: MAP(f: VARCHAR), g: INT), h: BIGINT)</tt>
 * <p>
 * Code:<pre><code>
 *     BatchSchema batchSchema = new SchemaBuilder()
 *        .add("c", MinorType.INT)
 *        .addMap("a")
 *          .addNullable("b", MinorType.VARCHAR)
 *          .add("d", MinorType.INT)
 *          .addMap("e")
 *            .add("f", MinorType.VARCHAR)
 *            .buildMap()
 *          .add("g", MinorType.INT)
 *          .buildMap()
 *        .addArray("h", MinorType.BIGINT)
 *        .build();
 * </code</pre>
 */

public class SchemaBuilder {

  /**
   * Build a column schema (AKA "materialized field") based on name and a
   * variety of schema options. Every column needs a name and (minor) type,
   * some may need a mode other than required, may need a width, may
   * need scale and precision, and so on.
   */

  public static class ColumnBuilder {
    private final String name;
    private final MajorType.Builder typeBuilder;

    public ColumnBuilder(String name, MinorType type) {
      this.name = name;
      typeBuilder = MajorType.newBuilder()
          .setMinorType(type)
          .setMode(DataMode.REQUIRED);
    }

    public ColumnBuilder setMode(DataMode mode) {
      typeBuilder.setMode(mode);
      return this;
    }

    public ColumnBuilder setWidth(int width) {
      return setPrecision(width);
    }

    public ColumnBuilder setPrecision(int precision) {
      typeBuilder.setPrecision(precision);
      return this;
    }

    public ColumnBuilder setScale(int scale, int precision) {
      typeBuilder.setScale(scale);
      typeBuilder.setPrecision(precision);
      return this;
    }

    public MaterializedField build() {
      return MaterializedField.create(name, typeBuilder.build());
    }
  }

  /**
   * Internal structure for building a map. A map is just a schema,
   * but one that is part of a parent column.
   */

  public static class MapBuilder extends SchemaBuilder {
    private final SchemaBuilder parent;
    private final String memberName;
    private final DataMode mode;

    public MapBuilder(SchemaBuilder parent, String memberName, DataMode mode) {
      this.parent = parent;
      this.memberName = memberName;
      // Optional maps not supported in Drill
      assert mode != DataMode.OPTIONAL;
      this.mode = mode;
    }

    @Override
    public BatchSchema build() {
      throw new IllegalStateException("Cannot build for a nested schema");
    }

    @Override
    public SchemaBuilder buildMap() {
      // TODO: Use the map schema directly rather than
      // rebuilding it as is done here.

      MaterializedField col = columnSchema(memberName, MinorType.MAP, mode);
      for (ColumnMetadata md : schema) {
        col.addChild(md.schema());
      }
      parent.finishMap(TupleSchema.newMap(col, schema));
      return parent;
    }

    @Override
    public SchemaBuilder withSVMode(SelectionVectorMode svMode) {
      throw new IllegalStateException("Cannot set SVMode for a nested schema");
    }
  }

  protected TupleSchema schema = new TupleSchema();
  private SelectionVectorMode svMode = SelectionVectorMode.NONE;

  /**
   * Create a new empty schema. Allows appending columns to it.
   */
  public SchemaBuilder() {}

  /**
   * Create a new schema starting with the base schema. Allows appending additional columns to the actual schema.
   */
  public SchemaBuilder(BatchSchema baseSchema) {
    for (MaterializedField field : baseSchema) {
      add(field);
    }
  }

  public SchemaBuilder add(String name, MajorType type) {
    return add(MaterializedField.create(name, type));
  }

  public SchemaBuilder add(MaterializedField col) {
    schema.add(col);
    return this;
  }

  /**
   * Create a column schema using the "basic three" properties of name, type and
   * cardinality (AKA "data mode.") Use the {@link ColumnBuilder} for to set
   * other schema attributes. Name is relative to the enclosing map or tuple;
   * it is not the fully qualified path name.
   */

  public static MaterializedField columnSchema(String name, MinorType type, DataMode mode) {
    return MaterializedField.create(name,
        MajorType.newBuilder()
          .setMinorType(type)
          .setMode(mode)
          .build());
  }

  public SchemaBuilder add(String name, MinorType type, DataMode mode) {
    return add(columnSchema(name, type, mode));
  }

  public SchemaBuilder add(String name, MinorType type) {
    return add(name, type, DataMode.REQUIRED);
  }

  public SchemaBuilder add(String name, MinorType type, int width) {
    MaterializedField field = new SchemaBuilder.ColumnBuilder(name, type)
        .setMode(DataMode.REQUIRED)
        .setWidth(width)
        .build();
    return add(field);
  }

  public SchemaBuilder addNullable(String name, MinorType type) {
    return add(name, type, DataMode.OPTIONAL);
  }

  public SchemaBuilder addNullable(String name, MinorType type, int width) {
    MaterializedField field = new SchemaBuilder.ColumnBuilder(name, type)
        .setMode(DataMode.OPTIONAL)
        .setWidth(width)
        .build();
    return add(field);
  }

  public SchemaBuilder addArray(String name, MinorType type) {
    return add(name, type, DataMode.REPEATED);
  }

  /**
   * Add a map column. The returned schema builder is for the nested
   * map. Building that map, using {@link MapBuilder#buildMap()},
   * will return the original schema builder.
   *
   * @param pathName the name of the map column
   * @return a builder for the map
   */

  public MapBuilder addMap(String pathName) {
    return new MapBuilder(this, pathName, DataMode.REQUIRED);
  }

  public MapBuilder addMapArray(String pathName) {
    return new MapBuilder(this, pathName, DataMode.REPEATED);
  }

  public SchemaBuilder withSVMode(SelectionVectorMode svMode) {
    this.svMode = svMode;
    return this;
  }

  public BatchSchema build() {
    return schema.toBatchSchema(svMode);
  }

  void finishMap(MapColumnMetadata map) {
    schema.add(map);
  }

  public SchemaBuilder buildMap() {
    throw new IllegalStateException("Cannot build map for a top-level schema");
  }

  public TupleMetadata buildSchema() {
    return schema;
  }
}
