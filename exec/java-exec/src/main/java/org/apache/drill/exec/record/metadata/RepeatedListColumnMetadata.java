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
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class RepeatedListColumnMetadata extends AbstractColumnMetadata {

  /**
   * Indicates we don't know the number of dimensions.
   */

  public static final int UNKNOWN_DIMENSIONS = -1;

  private AbstractColumnMetadata childSchema;

  public RepeatedListColumnMetadata(MaterializedField field) {
    super(field);
    Preconditions.checkArgument(field.getType().getMinorType() == MinorType.LIST);
    Preconditions.checkArgument(field.getType().getMode() == DataMode.REPEATED);
    Preconditions.checkArgument(field.getChildren().size() <= 1);
    if (! field.getChildren().isEmpty()) {
      childSchema = MetadataUtils.fromField(field.getChildren().iterator().next());
      Preconditions.checkArgument(childSchema.isArray());
    }
  }

  public RepeatedListColumnMetadata(String name, AbstractColumnMetadata childSchema) {
    super(name, MinorType.LIST, DataMode.REPEATED);
    if (childSchema != null) {
      Preconditions.checkArgument(childSchema.isArray());
    }
    this.childSchema = childSchema;
  }

  public void childSchema(ColumnMetadata childMetadata) {
    Preconditions.checkState(childSchema == null);
    Preconditions.checkArgument(childMetadata.mode() == DataMode.REPEATED);
    childSchema = (AbstractColumnMetadata) childMetadata;
  }

  @Override
  public StructureType structureType() { return StructureType.MULTI_ARRAY; }

  @Override
  public boolean isMultiList() { return true; }

  @Override
  public MaterializedField schema() {
    final MaterializedField field = emptySchema();
    if (childSchema != null) {
      field.addChild(childSchema.schema());
    }
    return field;
  }

  @Override
  public MaterializedField emptySchema() {
    return MaterializedField.create(name(), majorType());
  }

  @Override
  public ColumnMetadata cloneEmpty() {
    return new RepeatedListColumnMetadata(name, null);
  }

  @Override
  public AbstractColumnMetadata copy() {
    return new RepeatedListColumnMetadata(name, childSchema);
  }

  @Override
  public ColumnMetadata childSchema() { return childSchema; }

  @Override
  public int dimensions() {

    // If there is no child, then we don't know the
    // dimensionality.

    return childSchema == null ? UNKNOWN_DIMENSIONS
        : childSchema.dimensions() + 1;
  }
}
