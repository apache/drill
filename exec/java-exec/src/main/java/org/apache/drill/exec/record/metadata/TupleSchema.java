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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;

/**
 * Defines the schema of a tuple: either the top-level row or a nested
 * "map" (really structure). A schema is a collection of columns (backed
 * by vectors in the loader itself.) Columns are accessible by name or
 * index. New columns may be added at any time; the new column takes the
 * next available index.
 */

public class TupleSchema implements TupleMetadata {

  private MapColumnMetadata parentMap;
  private final TupleNameSpace<ColumnMetadata> nameSpace = new TupleNameSpace<>();

  public void bind(MapColumnMetadata parentMap) {
    this.parentMap = parentMap;
  }

  public TupleMetadata copy() {
    TupleMetadata tuple = new TupleSchema();
    for (ColumnMetadata md : this) {
      tuple.addColumn(((AbstractColumnMetadata) md).copy());
    }
    return tuple;
  }

  @Override
  public ColumnMetadata add(MaterializedField field) {
    AbstractColumnMetadata md = MetadataUtils.fromField(field);
    add(md);
    return md;
  }

  public ColumnMetadata addView(MaterializedField field) {
    AbstractColumnMetadata md = MetadataUtils.fromView(field);
    add(md);
    return md;
  }

  /**
   * Add a column metadata column created by the caller. Used for specialized
   * cases beyond those handled by {@link #add(MaterializedField)}.
   *
   * @param md the custom column metadata which must have the correct
   * index set (from {@link #size()}
   */

  public void add(AbstractColumnMetadata md) {
    md.bind(this);
    nameSpace.add(md.name(), md);
  }

  @Override
  public int addColumn(ColumnMetadata column) {
    add((AbstractColumnMetadata) column);
    return size() - 1;
  }

  @Override
  public MaterializedField column(String name) {
    ColumnMetadata md = metadata(name);
    return md == null ? null : md.schema();
  }

  @Override
  public ColumnMetadata metadata(String name) {
    return nameSpace.get(name);
  }

  @Override
  public int index(String name) {
    return nameSpace.indexOf(name);
  }

  @Override
  public MaterializedField column(int index) {
    return metadata(index).schema();
  }

  @Override
  public ColumnMetadata metadata(int index) {
    return nameSpace.get(index);
  }

  @Override
  public MapColumnMetadata parent() { return parentMap; }

  @Override
  public int size() { return nameSpace.count(); }

  @Override
  public boolean isEmpty() { return nameSpace.count( ) == 0; }

  @Override
  public Iterator<ColumnMetadata> iterator() {
    return nameSpace.iterator();
  }

  @Override
  public boolean isEquivalent(TupleMetadata other) {
    TupleSchema otherSchema = (TupleSchema) other;
    if (nameSpace.count() != otherSchema.nameSpace.count()) {
      return false;
    }
    for (int i = 0; i < nameSpace.count(); i++) {
      if (! nameSpace.get(i).isEquivalent(otherSchema.nameSpace.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public List<MaterializedField> toFieldList() {
    List<MaterializedField> cols = new ArrayList<>();
    for (ColumnMetadata md : nameSpace) {
      cols.add(md.schema());
    }
    return cols;
  }

  public BatchSchema toBatchSchema(SelectionVectorMode svMode) {
    return new BatchSchema(svMode, toFieldList());
  }

  @Override
  public String fullName(int index) {
    return fullName(metadata(index));
  }

  @Override
  public String fullName(ColumnMetadata column) {
    String quotedName = column.name();
    if (quotedName.contains(".")) {
      quotedName = "`" + quotedName + "`";
    }
    if (isRoot()) {
      return column.name();
    } else {
      return fullName() + "." + quotedName;
    }
  }

  public String fullName() {
    if (isRoot()) {
      return "<root>";
    } else {
      return parentMap.parentTuple().fullName(parentMap);
    }
  }

  public boolean isRoot() { return parentMap == null; }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder()
        .append("[")
        .append(getClass().getSimpleName())
        .append(" ");
    boolean first = true;
    for (ColumnMetadata md : nameSpace) {
      if (! first) {
        buf.append(", ");
      }
      buf.append(md.toString());
    }
    buf.append("]");
    return buf.toString();
  }
}
