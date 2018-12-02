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
package org.apache.drill.exec.record.metadata;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Describes a map and repeated map. Both are tuples that have a tuple
 * schema as part of the column definition.
 */
public class MapColumnMetadata extends AbstractColumnMetadata {

  private TupleMetadata parentTuple;
  private final TupleSchema mapSchema;

  /**
   * Build a new map column from the field provided
   *
   * @param schema materialized field description of the map
   */
  public MapColumnMetadata(MaterializedField schema) {
    this(schema, null);
  }

  /**
   * Build a map column metadata by cloning the type information (but not
   * the children) of the materialized field provided.
   *
   * @param schema the schema to use
   * @param mapSchema parent schema
   */
  MapColumnMetadata(MaterializedField schema, TupleSchema mapSchema) {
    super(schema);
    if (mapSchema == null) {
      this.mapSchema = new TupleSchema();
    } else {
      this.mapSchema = mapSchema;
    }
    this.mapSchema.bind(this);
  }

  public MapColumnMetadata(MapColumnMetadata from) {
    super(from);
    mapSchema = (TupleSchema) from.mapSchema.copy();
  }

  public MapColumnMetadata(String name, DataMode mode,
      TupleSchema mapSchema) {
    super(name, MinorType.MAP, mode);
    if (mapSchema == null) {
      this.mapSchema = new TupleSchema();
    } else {
      this.mapSchema = mapSchema;
    }
  }

  @Override
  public ColumnMetadata copy() {
    return new MapColumnMetadata(this);
  }

  @Override
  public void bind(TupleMetadata parentTuple) {
    this.parentTuple = parentTuple;
  }

  @Override
  public ColumnMetadata.StructureType structureType() { return ColumnMetadata.StructureType.TUPLE; }

  @Override
  public TupleMetadata mapSchema() { return mapSchema; }

  @Override
  public int expectedWidth() { return 0; }

  @Override
  public boolean isMap() { return true; }

  public TupleMetadata parentTuple() { return parentTuple; }

  @Override
  public ColumnMetadata cloneEmpty() {
    return new MapColumnMetadata(name, mode, new TupleSchema());
  }

  @Override
  public MaterializedField schema() {
    MaterializedField field = emptySchema();
    for (MaterializedField member : mapSchema.toFieldList()) {
      field.addChild(member);
    }
    return field;
  }

  @Override
  public MaterializedField emptySchema() {
   return MaterializedField.create(name,
        MajorType.newBuilder()
          .setMinorType(type)
          .setMode(mode)
          .build());
  }

  @Override
  public String typeString() {
    StringBuilder builder = new StringBuilder();
    if (isArray()) {
      builder.append("ARRAY<");
    }
    builder.append("MAP<").append(mapSchema.schemaString()).append(">");
    if (isArray()) {
      builder.append(">");
    }
    return builder.toString();
  }

}
