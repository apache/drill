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
package org.apache.drill.exec.physical.rowSet.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.drill.exec.physical.rowSet.ColumnLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.physical.rowSet.TupleSchema;
import org.apache.drill.exec.physical.rowSet.TupleSchema.TupleColumnSchema;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;

/**
 * Shim inserted between an actual tuple loader and the client to remove columns
 * that are not projected from input to output. The underlying loader handles only
 * the projected columns in order to improve efficiency. This class presents the
 * full table schema, but returns null for the non-projected columns. This allows
 * the reader to work with the table schema as defined by the data source, but
 * skip those columns which are not projected. Skipping non-projected columns avoids
 * creating value vectors which are immediately discarded. It may also save the reader
 * from reading unwanted data.
 */
public class LogicalTupleLoader implements TupleLoader {

  public static final int UNMAPPED = -1;

  private static class MappedColumn implements TupleColumnSchema {

    private final MaterializedField schema;
    private final int mapping;

    public MappedColumn(MaterializedField schema, int mapping) {
      this.schema = schema;
      this.mapping = mapping;
    }

    @Override
    public MaterializedField schema() { return schema; }

    @Override
    public boolean isSelected() { return mapping != UNMAPPED; }

    @Override
    public int vectorIndex() { return mapping; }
  }

  /**
   * Implementation of the tuple schema that describes the full data source
   * schema. The underlying loader schema is a subset of these columns. Note
   * that the columns appear in the same order in both schemas, but the loader
   * schema is a subset of the table schema.
   */

  private class LogicalTupleSchema implements TupleSchema {

    private final Set<String> selection = new HashSet<>();
    private final TupleSchema physicalSchema;

    private LogicalTupleSchema(TupleSchema physicalSchema, Collection<String> selection) {
      this.physicalSchema = physicalSchema;
      this.selection.addAll(selection);
    }

    @Override
    public int columnCount() { return logicalSchema.count(); }

    @Override
    public int columnIndex(String colName) {
      return logicalSchema.indexOf(rsLoader.toKey(colName));
    }

    @Override
    public TupleColumnSchema metadata(int colIndex) { return logicalSchema.get(colIndex); }

    @Override
    public MaterializedField column(int colIndex) { return logicalSchema.get(colIndex).schema(); }

    @Override
    public TupleColumnSchema metadata(String colName) { return logicalSchema.get(colName); }

    @Override
    public MaterializedField column(String colName) { return logicalSchema.get(colName).schema(); }

    @Override
    public int addColumn(MaterializedField columnSchema) {
      String key = rsLoader.toKey(columnSchema.getName());
      int pIndex;
      if (selection.contains(key)) {
        pIndex = physicalSchema.addColumn(columnSchema);
      } else {
        pIndex = UNMAPPED;
      }
      return logicalSchema.add(columnSchema.getName(), new MappedColumn(columnSchema, pIndex));
    }

    @Override
    public void setSchema(BatchSchema schema) {
      if (! logicalSchema.isEmpty()) {
        throw new IllegalStateException("Can only set schema when the tuple schema is empty");
      }
      for (MaterializedField field : schema) {
        addColumn(field);
      }
    }

    @Override
    public BatchSchema schema() {
      List<MaterializedField> fields = new ArrayList<>();
      for (int i = 0; i < columnCount(); i++) {
        fields.add(column(i));
      }
      return new BatchSchema(SelectionVectorMode.NONE, fields);
    }

    @Override
    public MaterializedSchema materializedSchema() {
      MaterializedSchema schema = new MaterializedSchema();
      for (int i = 0; i < columnCount(); i++) {
        schema.add(column(i));
      }
      return schema;
    }
  }

  private final ResultSetLoaderImpl rsLoader;
  private final LogicalTupleSchema schema;
  private final TupleLoader physicalLoader;
  private ColumnLoader mappingCache[];
  private final TupleNameSpace<MappedColumn> logicalSchema = new TupleNameSpace<>();

  public LogicalTupleLoader(ResultSetLoaderImpl rsLoader, TupleLoader physicalLoader, Collection<String> selection) {
    this.rsLoader = rsLoader;
    this.schema = new LogicalTupleSchema(physicalLoader.schema(), selection);
    this.physicalLoader = physicalLoader;
  }

  @Override
  public TupleSchema schema() { return schema; }

  @Override
  public ColumnLoader column(int colIndex) {
    if (mappingCache == null) {
      final int count = logicalSchema.count();
      mappingCache = new ColumnLoader[count];
      for (int i = 0; i < count; i++) {
        int pIndex = logicalSchema.get(i).mapping;
        if (pIndex != UNMAPPED) {
          mappingCache[i] = physicalLoader.column(pIndex);
        }
      }
    }
    return mappingCache[colIndex];
  }

  @Override
  public ColumnLoader column(String colName) {
    int lIndex = schema.columnIndex(rsLoader.toKey(colName));
    return lIndex == -1 ? null : column(lIndex);
  }

  @Override
  public TupleLoader loadRow(Object... values) {
    rsLoader.startRow();
    for (int i = 0; i < values.length;  i++) {
      set(i, values[i]);
    }
    rsLoader.saveRow();
    return this;
  }

  @Override
  public TupleLoader loadSingletonRow(Object value) {
    return loadRow(new Object[] {value});
  }

  @Override
  public void set(int colIndex, Object value) {
    ColumnLoader colLoader = column(colIndex);
    if (colLoader != null) {
      colLoader.set(value);
    }
  }
}
