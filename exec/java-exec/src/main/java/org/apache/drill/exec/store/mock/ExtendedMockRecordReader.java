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
package org.apache.drill.exec.store.mock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.mock.MockTableDef.MockColumn;
import org.apache.drill.exec.store.mock.MockTableDef.MockScanEntry;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;

/**
 * Extended form of the mock record reader that uses generator class
 * instances to create the mock values. This is a work in progress.
 * Generators exist for a few simple required types. One also exists
 * to generate strings that contain dates.
 * <p>
 * The definition is provided inside the sub scan used to create the
 * {@link ScanBatch} used to create this record reader.
 */

public class ExtendedMockRecordReader extends AbstractRecordReader {

  private ValueVector[] valueVectors;
  private int batchRecordCount;
  private int recordsRead;

  private final MockScanEntry config;
  private final ColumnDef fields[];

  public ExtendedMockRecordReader(MockScanEntry config) {
    this.config = config;

    fields = buildColumnDefs();
  }

  private ColumnDef[] buildColumnDefs() {
    List<ColumnDef> defs = new ArrayList<>();

    // Look for duplicate names. Bad things happen when the same name
    // appears twice. We must do this here because some tests create
    // a physical plan directly, meaning that this is the first
    // opportunity to review the column definitions.

    Set<String> names = new HashSet<>();
    MockColumn cols[] = config.getTypes();
    for (int i = 0; i < cols.length; i++) {
      MockTableDef.MockColumn col = cols[i];
      if (names.contains(col.name)) {
        throw new IllegalArgumentException("Duplicate column name: " + col.name);
      }
      names.add(col.name);
      int repeat = Math.min(1, col.getRepeatCount());
      if (repeat == 1) {
        defs.add(new ColumnDef(col));
      } else {
        for (int j = 0; j < repeat; j++) {
          defs.add(new ColumnDef(col, j+1));
        }
      }
    }
    ColumnDef[] defArray = new ColumnDef[defs.size()];
    defs.toArray(defArray);
    return defArray;
  }

  private int getEstimatedRecordSize() {
    int size = 0;
    for (int i = 0; i < fields.length; i++) {
      size += fields[i].width;
    }
    return size;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    try {
      final int estimateRowSize = getEstimatedRecordSize();
      valueVectors = new ValueVector[fields.length];
      int batchSize = config.getBatchSize();
      if (batchSize == 0) {
        batchSize = 10 * 1024 * 1024;
      }
      batchRecordCount = Math.max(1, batchSize / estimateRowSize);
      batchRecordCount = Math.min(batchRecordCount, Character.MAX_VALUE);

      for (int i = 0; i < fields.length; i++) {
        final ColumnDef col = fields[i];
        final MajorType type = col.getConfig().getMajorType();
        final MaterializedField field = MaterializedField.create(col.getName(), type);
        final Class<? extends ValueVector> vvClass = TypeHelper.getValueVectorClass(field.getType().getMinorType(), field.getDataMode());
        valueVectors[i] = output.addField(field, vvClass);
      }
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException("Failure while setting up fields", e);
    }
  }

  @Override
  public int next() {
    if (recordsRead >= this.config.getRecords()) {
      return 0;
    }

    final int recordSetSize = Math.min(batchRecordCount, this.config.getRecords() - recordsRead);
    recordsRead += recordSetSize;
    for (int i = 0; i < recordSetSize; i++) {
      int j = 0;
      for (final ValueVector v : valueVectors) {
        fields[j++].generator.setValue(v, i);
      }
    }

    return recordSetSize;
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    try {
      for (final ValueVector v : vectorMap.values()) {
        AllocationHelper.allocate(v, Character.MAX_VALUE, 50, 10);
      }
    } catch (NullPointerException e) {
      throw new OutOfMemoryException();
    }
  }

  @Override
  public void close() { }
}
