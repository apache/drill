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
 * Abstract definition of column metadata. Allows applications to create
 * specialized forms of a column metadata object by extending from this
 * abstract class.
 * <p>
 * Note that, by design, primitive columns do not have a link to their
 * tuple parent, or their index within that parent. This allows the same
 * metadata to be shared between two views of a tuple, perhaps physical
 * and projected views. This restriction does not apply to map columns,
 * since maps (and the row itself) will, by definition, differ between
 * the two views.
 */

public abstract class AbstractColumnMetadata implements ColumnMetadata {

  // Capture the key schema information. We cannot use the MaterializedField
  // or MajorType because then encode child information that we encode here
  // as a child schema. Keeping the two in sync is nearly impossible.

  protected final String name;
  protected final MinorType type;
  protected final DataMode mode;
  protected final int precision;
  protected final int scale;
  protected boolean projected = true;

  /**
   * Predicted number of elements per array entry. Default is
   * taken from the often hard-coded value of 10.
   */

  protected int expectedElementCount = 1;

  public AbstractColumnMetadata(MaterializedField schema) {
    name = schema.getName();
    MajorType majorType = schema.getType();
    type = majorType.getMinorType();
    mode = majorType.getMode();
    precision = majorType.getPrecision();
    scale = majorType.getScale();
    if (isArray()) {
      expectedElementCount = DEFAULT_ARRAY_SIZE;
    }
  }

  public AbstractColumnMetadata(String name, MinorType type, DataMode mode) {
    this.name = name;
    this.type = type;
    this.mode = mode;
    precision = 0;
    scale = 0;
    if (isArray()) {
      expectedElementCount = DEFAULT_ARRAY_SIZE;
    }
  }

  public AbstractColumnMetadata(AbstractColumnMetadata from) {
    name = from.name;
    type = from.type;
    mode = from.mode;
    precision = from.precision;
    scale = from.scale;
    expectedElementCount = from.expectedElementCount;
  }

  protected void bind(TupleSchema parentTuple) { }

  @Override
  public String name() { return name; }

  @Override
  public MinorType type() { return type; }

  @Override
  public MajorType majorType() {
    return MajorType.newBuilder()
        .setMinorType(type())
        .setMode(mode())
        .build();
  }

  @Override
  public DataMode mode() { return mode; }

  @Override
  public boolean isNullable() { return mode() == DataMode.OPTIONAL; }

  @Override
  public boolean isArray() { return mode() == DataMode.REPEATED; }

  @Override
  public int dimensions() { return isArray() ? 1 : 0; }

  @Override
  public boolean isMap() { return false; }

  @Override
  public boolean isVariant() { return false; }

  @Override
  public TupleMetadata mapSchema() { return null; }

  @Override
  public VariantMetadata variantSchema() { return null; }

  @Override
  public ColumnMetadata childSchema() { return null; }

  @Override
  public boolean isVariableWidth() {
    MinorType type = type();
    return type == MinorType.VARCHAR || type == MinorType.VAR16CHAR || type == MinorType.VARBINARY;
  }

  @Override
  public boolean isEquivalent(ColumnMetadata other) {
    return schema().isEquivalent(other.schema());
  }

  @Override
  public int expectedWidth() { return 0; }

  @Override
  public void setExpectedWidth(int width) { }

  @Override
  public int precision() { return 0; }

  @Override
  public int scale() { return 0; }

  @Override
  public void setExpectedElementCount(int childCount) {
    // The allocation utilities don't like an array size of zero, so set to
    // 1 as the minimum. Adjusted to avoid trivial errors if the caller
    // makes an error.

    if (isArray()) {
      expectedElementCount = Math.max(1, childCount);
    }
  }

  @Override
  public int expectedElementCount() { return expectedElementCount; }

  @Override
  public void setProjected(boolean projected) {
    this.projected = projected;
  }

  @Override
  public boolean isProjected() { return projected; }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder()
        .append("[")
        .append(getClass().getSimpleName())
        .append(" ")
        .append(schema().toString())
        .append(", ")
        .append(projected ? "" : "not ")
        .append("projected");
    if (isArray()) {
      buf.append(", cardinality: ")
         .append(expectedElementCount);
    }
    if (variantSchema() != null) {
      buf.append(", variant: ")
         .append(variantSchema().toString());
    }
    if (mapSchema() != null) {
      buf.append(", schema: ")
         .append(mapSchema().toString());
    }
    return buf
        .append("]")
        .toString();
  }

  public abstract AbstractColumnMetadata copy();
}