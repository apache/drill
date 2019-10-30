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
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.vector.complex.DictVector;

/**
 * Internal structure for building a dict. Dict is an array of key-value pairs
 * with defined types for key and value (key and value fields are defined within {@link TupleSchema}).
 * Key can be {@link org.apache.drill.common.types.TypeProtos.DataMode#REQUIRED} primitive,
 * while value can be primitive or complex.
 * <p>Column is added to the parent container during creation
 * and all <tt>resumeXXX</tt> methods return qualified parent container.</p>
 *
 * @see DictVector
 */
public class DictBuilder implements SchemaContainer {

  /**
   * Schema containing key and value fields' definition.
   */
  private final TupleSchema schema = new TupleSchema();
  private final SchemaContainer parent;
  private final String name;
  private final TypeProtos.DataMode mode;

  private TypeProtos.MajorType keyType;
  private TypeProtos.MajorType valueType;

  public DictBuilder(SchemaContainer parent, String name, TypeProtos.DataMode mode) {
    this.parent = parent;
    this.name = name;
    this.mode = mode;
  }

  @Override
  public void addColumn(ColumnMetadata column) {
    assert DictVector.fieldNames.contains(column.name())
        : String.format("Dict consists of two columns: %s and %s. Found: %s.",
            DictVector.FIELD_KEY_NAME, DictVector.FIELD_VALUE_NAME, column.name());
    assert schema.metadata(column.name()) == null
        : String.format("Field %s is already defined in dict.", column.name());

    TypeProtos.MajorType columnType = column.majorType();
    if (DictVector.FIELD_KEY_NAME.equals(column.name())) {
      assert isSupportedKeyType(columnType) : "Key in dict should be non-nullable primitive. Found: " + columnType;
      keyType = columnType;
    } else {
      valueType = columnType;
    }

    schema.addColumn(column);
  }

  public DictBuilder key(TypeProtos.MinorType type) {
    TypeProtos.MajorType keyType = Types.withMode(type, TypeProtos.DataMode.REQUIRED);
    return key(keyType);
  }

  public DictBuilder key(TypeProtos.MajorType type) {
    if (keyType != null) {
      throw new IllegalStateException("Key field is already defined.");
    }

    if (!isSupportedKeyType(type)) {
      throw new IllegalArgumentException("Key in dict should be non-nullable primitive. Found: " + type);
    }

    keyType = type;
    return field(DictVector.FIELD_KEY_NAME, keyType);
  }

  public DictBuilder value(TypeProtos.MinorType type, TypeProtos.DataMode mode) {
    TypeProtos.MajorType valueType = Types.withMode(type, mode);
    return value(valueType);
  }

  public DictBuilder value(TypeProtos.MajorType type) {
    if (valueType != null) {
      throw new IllegalStateException("Value field is already defined.");
    }

    valueType = type;
    return field(DictVector.FIELD_VALUE_NAME, valueType);
  }

  private DictBuilder field(String name, TypeProtos.MajorType type) {
    ColumnBuilder builder = new ColumnBuilder(name, type.getMinorType())
        .setMode(type.getMode());

    if (type.hasScale()) {
      builder.setPrecisionAndScale(type.getPrecision(), type.getScale());
    } else if (type.hasPrecision()) {
      builder.setPrecision(type.getPrecision());
    }
    if (type.hasWidth()) {
      builder.setWidth(type.getWidth());
    }

    schema.add(builder.build());
    return this;
  }

  public MapBuilder mapValue() {
    return new MapBuilder(this, DictVector.FIELD_VALUE_NAME, TypeProtos.DataMode.REQUIRED);
  }

  public MapBuilder mapArrayValue() {
    return new MapBuilder(this, DictVector.FIELD_VALUE_NAME, TypeProtos.DataMode.REPEATED);
  }

  public DictBuilder dictValue() {
    return new DictBuilder(this, DictVector.FIELD_VALUE_NAME, TypeProtos.DataMode.REQUIRED);
  }

  public DictBuilder dictArrayValue() {
    return new DictBuilder(this, DictVector.FIELD_VALUE_NAME, TypeProtos.DataMode.REPEATED);
  }

  public UnionBuilder unionValue() {
    return new UnionBuilder(this, DictVector.FIELD_VALUE_NAME, TypeProtos.MinorType.UNION);
  }

  public UnionBuilder listValue() {
    return new UnionBuilder(this, DictVector.FIELD_VALUE_NAME, TypeProtos.MinorType.LIST);
  }

  public RepeatedListBuilder repeatedListValue() {
    return new RepeatedListBuilder(this, DictVector.FIELD_VALUE_NAME);
  }

  public void build() {
    if (parent != null) {
      DictColumnMetadata columnMetadata = new DictColumnMetadata(name, mode, schema);
      parent.addColumn(columnMetadata);
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

  private boolean isSupportedKeyType(TypeProtos.MajorType type) {
    return !Types.isComplex(type)
        && !Types.isUnion(type)
        && type.getMode() == TypeProtos.DataMode.REQUIRED;
  }
}
