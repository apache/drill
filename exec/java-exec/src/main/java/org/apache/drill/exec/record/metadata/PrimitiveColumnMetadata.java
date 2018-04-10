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
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Primitive (non-map) column. Describes non-nullable, nullable and
 * array types (which differ only in mode, but not in metadata structure.)
 */

public class PrimitiveColumnMetadata extends AbstractColumnMetadata {

  protected int expectedWidth;

  public PrimitiveColumnMetadata(MaterializedField schema) {
    super(schema);
    expectedWidth = estimateWidth(schema.getType());
  }

  public PrimitiveColumnMetadata(String name, MinorType type, DataMode mode) {
    super(name, type, mode);
    expectedWidth = estimateWidth(Types.withMode(type, mode));
  }

  private int estimateWidth(MajorType majorType) {
    if (type() == MinorType.NULL || type() == MinorType.LATE) {
      return 0;
    } else if (isVariableWidth()) {

      // The above getSize() method uses the deprecated getWidth()
      // method to get the expected VarChar size. If zero (which
      // it will be), try the revised precision field.

      int precision = majorType.getPrecision();
      if (precision > 0) {
        return precision;
      } else {
        // TypeHelper includes the offset vector width

        return TypeHelper.getSize(majorType) - 4;
      }
    } else {
      return TypeHelper.getSize(majorType);
    }
  }

  public PrimitiveColumnMetadata(PrimitiveColumnMetadata from) {
    super(from);
    expectedWidth = from.expectedWidth;
  }

  @Override
  public AbstractColumnMetadata copy() {
    return new PrimitiveColumnMetadata(this);
  }

  @Override
  public ColumnMetadata.StructureType structureType() { return ColumnMetadata.StructureType.PRIMITIVE; }

  @Override
  public int expectedWidth() { return expectedWidth; }

  @Override
  public int precision() { return precision; }

  @Override
  public int scale() { return scale; }

  @Override
  public void setExpectedWidth(int width) {
    // The allocation utilities don't like a width of zero, so set to
    // 1 as the minimum. Adjusted to avoid trivial errors if the caller
    // makes an error.

    if (isVariableWidth()) {
      expectedWidth = Math.max(1, width);
    }
  }

  @Override
  public ColumnMetadata cloneEmpty() {
    return new PrimitiveColumnMetadata(this);
  }

  public ColumnMetadata mergeWith(MaterializedField field) {
    PrimitiveColumnMetadata merged = new PrimitiveColumnMetadata(field);
    merged.setExpectedElementCount(expectedElementCount);
    merged.setExpectedWidth(Math.max(expectedWidth, field.getPrecision()));
    merged.setProjected(projected);
    return merged;
  }

  @Override
  public MajorType majorType() {
    return MajorType.newBuilder()
        .setMinorType(type)
        .setMode(mode)
        .setPrecision(precision)
        .setScale(scale)
        .build();
  }

  @Override
  public MaterializedField schema() {
    return MaterializedField.create(name, majorType());
  }

  @Override
  public MaterializedField emptySchema() { return schema(); }
}
