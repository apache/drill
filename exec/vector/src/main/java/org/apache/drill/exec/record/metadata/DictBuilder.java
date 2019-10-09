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

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.DictVector;

public class DictBuilder implements SchemaContainer {

  private final SchemaContainer parent;
  private final TupleBuilder tupleBuilder = new TupleBuilder();
  private final String memberName;
  private final TypeProtos.DataMode mode;

  public DictBuilder(String memberName, TypeProtos.DataMode mode) {
    this(null, memberName, mode);
  }

  public DictBuilder(SchemaContainer parent, String memberName, TypeProtos.DataMode mode) {
    this.parent = parent;
    this.memberName = memberName;
    this.mode = mode;
  }

  @Override
  public void addColumn(ColumnMetadata column) {
    assert DictVector.fieldNames.contains(column.name());
    tupleBuilder.addColumn(column);
  }

  public DictBuilder addKey(TypeProtos.MajorType type) {
    return add(MaterializedField.create(DictVector.FIELD_KEY_NAME, type));
  }

  public DictBuilder addValue(TypeProtos.MajorType type) {
    return add(MaterializedField.create(DictVector.FIELD_VALUE_NAME, type));
  }

  public DictBuilder add(MaterializedField col) {
    assert DictVector.fieldNames.contains(col.getName());
    tupleBuilder.add(col);
    return this;
  }

  public DictBuilder addKey(TypeProtos.MinorType type, TypeProtos.DataMode mode) {
    tupleBuilder.add(DictVector.FIELD_KEY_NAME, type, mode);
    return this;
  }

  public DictBuilder addValue(TypeProtos.MinorType type, TypeProtos.DataMode mode) {
    tupleBuilder.add(DictVector.FIELD_VALUE_NAME, type, mode);
    return this;
  }

  public DictBuilder addKey(TypeProtos.MinorType type) {
    tupleBuilder.add(DictVector.FIELD_KEY_NAME, type);
    return this;
  }

  public DictBuilder addValue(TypeProtos.MinorType type) {
    tupleBuilder.add(DictVector.FIELD_VALUE_NAME, type);
    return this;
  }

  public DictBuilder addKey(TypeProtos.MinorType type, int width) {
    tupleBuilder.add(DictVector.FIELD_KEY_NAME, type, width);
    return this;
  }

  public DictBuilder addValue(TypeProtos.MinorType type, int width) {
    tupleBuilder.add(DictVector.FIELD_VALUE_NAME, type, width);
    return this;
  }

  public DictBuilder addKey(TypeProtos.MinorType type, int precision, int scale) {
    return addDecimal(DictVector.FIELD_KEY_NAME, type, TypeProtos.DataMode.REQUIRED, precision, scale);
  }

  public DictBuilder addValue(TypeProtos.MinorType type, int precision, int scale) {
    return addDecimal(DictVector.FIELD_VALUE_NAME, type, TypeProtos.DataMode.REQUIRED, precision, scale);
  }

  public DictBuilder addNullable(String name, TypeProtos.MinorType type) {
    tupleBuilder.addNullable(name,  type);
    return this;
  }

  public DictBuilder addNullable(String name, TypeProtos.MinorType type, int width) {
    tupleBuilder.addNullable(name, type, width);
    return this;
  }

  public DictBuilder addNullable(String name, TypeProtos.MinorType type, int precision, int scale) {
    return addDecimal(name, type, TypeProtos.DataMode.OPTIONAL, precision, scale);
  }

  public DictBuilder addArrayValue(TypeProtos.MinorType type) {
    tupleBuilder.addArray(DictVector.FIELD_VALUE_NAME, type);
    return this;
  }

  public DictBuilder addArrayValue(TypeProtos.MinorType type, int dims) {
    tupleBuilder.addArray(DictVector.FIELD_VALUE_NAME, type, dims);
    return this;
  }

  public DictBuilder addArrayValue(TypeProtos.MinorType type, int precision, int scale) {
    return addDecimal(DictVector.FIELD_VALUE_NAME, type, TypeProtos.DataMode.REPEATED, precision, scale);
  }

  public DictBuilder addDecimal(String name, TypeProtos.MinorType type,
                               TypeProtos.DataMode mode, int precision, int scale) {
    tupleBuilder.addDecimal(name, type, mode, precision, scale);
    return this;
  }

  /**
   * Add a map column as dict's value. The returned schema builder is for the nested
   * map. Building that map, using {@link DictBuilder#resumeSchema()},
   * will return the original schema builder.
   *
   * @return a builder for the map
   */
  public MapBuilder addMapValue() {
    return tupleBuilder.addMap(this, DictVector.FIELD_VALUE_NAME);
  }

  public MapBuilder addMapArrayValue() {
    return tupleBuilder.addMapArray(this, DictVector.FIELD_VALUE_NAME);
  }

  public DictBuilder addDictValue() {
    return tupleBuilder.addDict(this, DictVector.FIELD_VALUE_NAME);
  }

  public UnionBuilder addUnionValue() {
    return tupleBuilder.addUnion(this, DictVector.FIELD_VALUE_NAME);
  }

  public UnionBuilder addListValue() {
    return tupleBuilder.addList(this, DictVector.FIELD_VALUE_NAME);
  }

  public RepeatedListBuilder addRepeatedListValue() {
    return tupleBuilder.addRepeatedList(this, DictVector.FIELD_VALUE_NAME);
  }

  public DictColumnMetadata buildColumn() {
    return new DictColumnMetadata(memberName, mode, tupleBuilder.schema());
  }

  public void build() {
    if (parent != null) {
      parent.addColumn(buildColumn());
    }
  }

  public SchemaBuilder resumeSchema() {
    build();
    return (SchemaBuilder) parent;
  }

  public DictBuilder resumeMap() {
    build();
    return (DictBuilder) parent;
  }

  public RepeatedListBuilder resumeList() {
    build();
    return (RepeatedListBuilder) parent;
  }

  public UnionBuilder resumeUnion() {
    build();
    return (UnionBuilder) parent;
  }

  public DictBuilder resumeDict() {
    build();
    return (DictBuilder) parent;
  }
}
