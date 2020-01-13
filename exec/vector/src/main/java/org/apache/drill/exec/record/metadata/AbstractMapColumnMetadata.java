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

import java.util.stream.Collectors;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Describes a base column type for map, dict, repeated map and repeated dict. All are tuples that have a tuple
 * schema as part of the column definition.
 */
public abstract class AbstractMapColumnMetadata extends AbstractColumnMetadata {

  protected TupleMetadata parentTuple;
  protected final TupleSchema schema;

  /**
   * Build a new map column from the field provided
   *
   * @param schema materialized field description of the map
   */
  public AbstractMapColumnMetadata(MaterializedField schema) {
    this(schema, null);
  }

  /**
   * Build column metadata by cloning the type information (but not
   * the children) of the materialized field provided.
   *
   * @param schema the schema to use
   * @param mapSchema parent schema
   */
  AbstractMapColumnMetadata(MaterializedField schema, TupleSchema mapSchema) {
    super(schema);
    if (mapSchema == null) {
      this.schema = new TupleSchema();
    } else {
      this.schema = mapSchema;
    }
    this.schema.bind(this);
  }

  public AbstractMapColumnMetadata(AbstractMapColumnMetadata from) {
    super(from);
    schema = (TupleSchema) from.schema.copy();
  }

  public AbstractMapColumnMetadata(String name, MinorType type, DataMode mode, TupleSchema schema) {
    super(name, type, mode);
    if (schema == null) {
      this.schema = new TupleSchema();
    } else {
      this.schema = schema;
    }
  }

  @Override
  public void bind(TupleMetadata parentTuple) {
    this.parentTuple = parentTuple;
  }

  @Override
  public ColumnMetadata.StructureType structureType() {
    return ColumnMetadata.StructureType.TUPLE;
  }

  @Override
  public TupleMetadata tupleSchema() {
    return schema;
  }

  @Override
  public int expectedWidth() {
    return 0;
  }

  public TupleMetadata parentTuple() {
    return parentTuple;
  }

  @Override
  public MaterializedField schema() {
    MaterializedField field = emptySchema();
    for (MaterializedField member : schema.toFieldList()) {
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
    builder.append(getStringType())
        .append("<").append(
            tupleSchema().toMetadataList().stream()
              .map(ColumnMetadata::columnString)
              .collect(Collectors.joining(", "))
        )
        .append(">");
    if (isArray()) {
      builder.append(">");
    }
    return builder.toString();
  }

  /**
   * Returns string representation of type like {@code "STRUCT"} or {@code "MAP"}
   * @return column type
   */
  protected abstract String getStringType();

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    AbstractMapColumnMetadata other = (AbstractMapColumnMetadata) o;
    return schema.equals(other.mapSchema());
  }
}
