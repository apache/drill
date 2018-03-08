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
package org.apache.drill.test.rowSet.schema;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.AbstractColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;

/**
 * Builder of a row set schema expressed as a list of materialized
 * fields. Optimized for use when creating schemas by hand in tests.
 * <p>
 * Example usage to create the following schema: <br>
 * <tt>(c: INT, a: MAP(b: VARCHAR, d: INT, e: MAP(f: VARCHAR), g: INT),
 * h: UNION(INT, MAP(h1: INT), LIST(BIGINT)),
 * i: BIGINT[], j: VARCHAR[][][])</tt>
 * <p>
 * Code:<pre><code>
 *     BatchSchema batchSchema = new SchemaBuilder()
 *        .add("c", MinorType.INT)
 *        .addMap("a")
 *          .addNullable("b", MinorType.VARCHAR)
 *          .add("d", MinorType.INT)
 *          .addMap("e") // or .addMapArray("e")
 *            .add("f", MinorType.VARCHAR)
 *            .buildMap()
 *          .add("g", MinorType.INT)
 *          .buildMap()
 *        .addUnion("h") // or .addList("h")
 *          .addType(MinorType.INT)
 *          .addMap()
 *            .add("h1", MinorType.INT)
 *            .buildNested()
 *          .addList()
 *            .addType(MinorType.BIGINT)
 *            .buildNested()
 *          .build()
 *        .addArray("i", MinorType.BIGINT)
 *        .addRepeatedList("j")
 *          .addDimension()
 *            .addArray(MinorType.VARCHAR)
 *            .endDimension()
 *         .build()
 *        .build();
 * </code</pre>
 */

public class SchemaBuilder implements SchemaContainer {

  /**
   * Actual tuple schema builder. The odd layered structure is needed
   * so that the return value of each method is the builder
   * itself. We have two: one for the top-level schema, another for
   * maps. (The list, repeated list, and union builders are similar,
   * but they are each unique, so don't share "guts".)
   */

  private TupleBuilder tupleBuilder = new TupleBuilder();
  private SelectionVectorMode svMode = SelectionVectorMode.NONE;

  public SchemaBuilder() { }

  /**
   * Create a new schema starting with the base schema. Allows appending
   * additional columns to an additional schema.
   */

  public SchemaBuilder(BatchSchema baseSchema) {
    for (MaterializedField field : baseSchema) {
      add(field);
    }
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

  @Override
  public void addColumn(AbstractColumnMetadata column) {
    tupleBuilder.addColumn(column);
  }

  public SchemaBuilder add(String name, MajorType type) {
    return add(MaterializedField.create(name, type));
  }

  public SchemaBuilder add(MaterializedField col) {
    tupleBuilder.add(col);
    return this;
  }

  public SchemaBuilder add(String name, MinorType type, DataMode mode) {
    tupleBuilder.add(name, type, mode);
    return this;
  }

  public SchemaBuilder add(String name, MinorType type) {
    tupleBuilder.add(name, type);
    return this;
  }

  public SchemaBuilder add(String name, MinorType type, int width) {
    tupleBuilder.add(name, type, width);
    return this;
  }

  public SchemaBuilder addNullable(String name, MinorType type) {
    tupleBuilder.addNullable(name,  type);
    return this;
  }

  public SchemaBuilder addNullable(String name, MinorType type, int width) {
    tupleBuilder.addNullable(name, type, width);
    return this;
  }

  public SchemaBuilder addArray(String name, MinorType type) {
    tupleBuilder.addArray(name, type);
    return this;
  }

  public SchemaBuilder addDecimal(String name, MinorType type, DataMode mode, int precision, int scale) {
    tupleBuilder.addDecimal(name, type, mode, precision, scale);
    return this;
  }

  /**
   * Add a multi-dimensional array, implemented as a repeated vector
   * along with 0 or more repeated list vectors.
   *
   * @param name column name
   * @param type base data type
   * @param dims number of dimensions, 1 or more
   * @return this builder
   */

  public SchemaBuilder addArray(String name, MinorType type, int dims) {
    tupleBuilder.addArray(name,  type, dims);
    return this;
  }

  /**
   * Add a map column. The returned schema builder is for the nested
   * map. Building that map, using {@link MapBuilder#resumeSchema()},
   * will return the original schema builder.
   *
   * @param pathName the name of the map column
   * @return a builder for the map
   */

  public MapBuilder addMap(String name) {
    return tupleBuilder.addMap(this, name);
  }

  public MapBuilder addMapArray(String name) {
    return tupleBuilder.addMapArray(this, name);
  }

  public UnionBuilder addUnion(String name) {
    return tupleBuilder.addUnion(this, name);
  }

  public UnionBuilder addList(String name) {
    return tupleBuilder.addList(this, name);
  }

  public RepeatedListBuilder addRepeatedList(String name) {
    return tupleBuilder.addRepeatedList(this, name);
  }

  public SchemaBuilder withSVMode(SelectionVectorMode svMode) {
    this.svMode = svMode;
    return this;
  }

  public BatchSchema build() {
    return tupleBuilder.batchSchema(svMode);
  }

  public TupleMetadata buildSchema() {
    return tupleBuilder.schema();
  }
}
