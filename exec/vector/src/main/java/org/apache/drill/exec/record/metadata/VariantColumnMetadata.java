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

import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;

public class VariantColumnMetadata extends AbstractColumnMetadata {

  private final VariantSchema variantSchema;

  public VariantColumnMetadata(MaterializedField schema) {
    super(schema);
    variantSchema = new VariantSchema();
    variantSchema.bind(this);
    List<MinorType> types;
    if (type() == MinorType.UNION) {
      types = schema.getType().getSubTypeList();
    } else {
      assert type() == MinorType.LIST;
      MaterializedField child;
      MinorType childType;
      if (schema.getChildren().isEmpty()) {
        child = null;
        childType = MinorType.LATE;
      } else {
        child = schema.getChildren().iterator().next();
        childType = child.getType().getMinorType();
      }
      switch (childType) {
      case UNION:

        // List contains a union.

        types = child.getType().getSubTypeList();
        break;

      case LATE:

        // List has no type.

        return;

      default:

        // List contains a single non-null type.

        variantSchema.addType(MetadataUtils.fromField(child));
        return;
      }
    }
    if (types == null) {
      return;
    }
    for (MinorType type : types) {
      variantSchema.addType(type);
    }
  }

  public VariantColumnMetadata(MaterializedField schema, VariantSchema variantSchema) {
    super(schema);
    this.variantSchema = variantSchema;
  }

  public VariantColumnMetadata(String name, MinorType type, VariantSchema variantSchema) {
    super(name, type, DataMode.OPTIONAL);
    this.variantSchema = variantSchema == null ? new VariantSchema() : variantSchema;
    this.variantSchema.bind(this);
  }

  @Override
  public StructureType structureType() {
    return StructureType.VARIANT;
  }

  @Override
  public boolean isVariant() { return true; }

  @Override
  public boolean isArray() { return type() == MinorType.LIST; }

  @Override
  public ColumnMetadata cloneEmpty() {
    return new VariantColumnMetadata(name, type, variantSchema.cloneEmpty());
  }

  @Override
  public ColumnMetadata copy() {
    // TODO Auto-generated method stub
    assert false;
    return null;
  }

  @Override
  public VariantMetadata variantSchema() {
    return variantSchema;
  }

  @Override
  public MaterializedField schema() {
    return MaterializedField.create(name,
        MajorType.newBuilder()
          .setMinorType(type)
          .setMode(DataMode.OPTIONAL)
          .addAllSubType(variantSchema.types())
          .build());
  }

  @Override
  public MaterializedField emptySchema() {
    return MaterializedField.create(name,
        MajorType.newBuilder()
          .setMinorType(type)
          .setMode(DataMode.OPTIONAL)
          .build());
  }
}
