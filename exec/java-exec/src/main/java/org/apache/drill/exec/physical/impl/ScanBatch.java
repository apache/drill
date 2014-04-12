/**
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
package org.apache.drill.exec.physical.impl;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.util.BatchPrinter;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Maps;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

/**
 * Record batch used for a particular scan. Operators against one or more
 */
public class ScanBatch implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanBatch.class);

  private static final long ALLOCATOR_INITIAL_RESERVATION = 1*1024*1024;
  private static final long ALLOCATOR_MAX_RESERVATION = 20L*1000*1000*1000;

  final Map<MaterializedField, ValueVector> fieldVectorMap = Maps.newHashMap();

  private final VectorContainer container = new VectorContainer();
  private int recordCount;
  private boolean schemaChanged = true;
  private final FragmentContext context;
  private final OperatorContext oContext;
  private Iterator<RecordReader> readers;
  private RecordReader currentReader;
  private BatchSchema schema;
  private final Mutator mutator = new Mutator();
  private Iterator<String[]> partitionColumns;
  private String[] partitionValues;
  List<ValueVector> partitionVectors;
  List<Integer> selectedPartitionColumns;
  private String partitionColumnDesignator;

  public ScanBatch(PhysicalOperator subScanConfig, FragmentContext context, Iterator<RecordReader> readers, List<String[]> partitionColumns, List<Integer> selectedPartitionColumns) throws ExecutionSetupException {
    this.context = context;
    this.readers = readers;
    if (!readers.hasNext())
      throw new ExecutionSetupException("A scan batch must contain at least one reader.");
    this.currentReader = readers.next();
    this.oContext = new OperatorContext(subScanConfig, context);
    this.currentReader.setup(mutator);
    this.partitionColumns = partitionColumns.iterator();
    this.partitionValues = this.partitionColumns.hasNext() ? this.partitionColumns.next() : null;
    this.selectedPartitionColumns = selectedPartitionColumns;
    DrillConfig config = context.getConfig(); //This nonsense it is to not break all the stupid unit tests using SimpleRootExec
    this.partitionColumnDesignator = config == null ? "dir" : config.getString(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL);
    addPartitionVectors();
  }

  public ScanBatch(PhysicalOperator subScanConfig, FragmentContext context, Iterator<RecordReader> readers) throws ExecutionSetupException {
    this(subScanConfig, context, readers, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  public void kill() {
    releaseAssets();
  }

  private void releaseAssets() {
    container.clear();
  }

  @Override
  public IterOutcome next() {
    while ((recordCount = currentReader.next()) == 0) {
      try {
        if (!readers.hasNext()) {
          currentReader.cleanup();
          releaseAssets();
          return IterOutcome.NONE;
        }
        currentReader.cleanup();
        currentReader = readers.next();
        partitionValues = partitionColumns.hasNext() ? partitionColumns.next() : null;
        mutator.removeAllFields();
        currentReader.setup(mutator);
        addPartitionVectors();
      } catch (ExecutionSetupException e) {
        this.context.fail(e);
        releaseAssets();
        return IterOutcome.STOP;
      }
    }

    populatePartitionVectors();
    if (schemaChanged) {
      schemaChanged = false;
      return IterOutcome.OK_NEW_SCHEMA;
    } else {
      return IterOutcome.OK;
    }
  }

  private void addPartitionVectors() {
    partitionVectors = Lists.newArrayList();
    for (int i : selectedPartitionColumns) {
      MaterializedField field;
      ValueVector v;
      if (partitionValues.length > i) {
        field = MaterializedField.create(SchemaPath.getSimplePath(partitionColumnDesignator + i), Types.required(MinorType.VARCHAR));
        v = new VarCharVector(field, context.getAllocator());
      } else {
        field = MaterializedField.create(SchemaPath.getSimplePath(partitionColumnDesignator + i), Types.optional(MinorType.VARCHAR));
        v = new NullableVarCharVector(field, context.getAllocator());
      }
      mutator.addField(v);
      partitionVectors.add(v);
    }
  }

  private void populatePartitionVectors() {
    for (int i : selectedPartitionColumns) {
      if (partitionValues.length > i) {
        VarCharVector v = (VarCharVector) partitionVectors.get(i);
        String val = partitionValues[i];
        byte[] bytes = val.getBytes();
        AllocationHelper.allocate(v, recordCount, val.length());
        for (int j = 0; j < recordCount; j++) {
          v.getMutator().setSafe(j, bytes);
        }
        v.getMutator().setValueCount(recordCount);
      } else {
        NullableVarCharVector v = (NullableVarCharVector) partitionVectors.get(i);
        AllocationHelper.allocate(v, recordCount, 0);
        v.getMutator().setValueCount(recordCount);
      }
    }
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return container.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(int fieldId, Class<?> clazz) {
    return container.getValueAccessorById(fieldId, clazz);
  }



  private class Mutator implements OutputMutator {

    public void removeField(MaterializedField field) throws SchemaChangeException {
      ValueVector vector = fieldVectorMap.remove(field);
      if (vector == null) throw new SchemaChangeException("Failure attempting to remove an unknown field.");
      container.remove(vector);
      vector.close();
    }

    public void addField(ValueVector vector) {
      container.add(vector);
      fieldVectorMap.put(vector.getField(), vector);
    }

    @Override
    public void removeAllFields() {
      for(VectorWrapper<?> vw : container){
        vw.clear();
      }
      container.clear();
      fieldVectorMap.clear();
    }

    @Override
    public void setNewSchema() throws SchemaChangeException {
      container.buildSchema(SelectionVectorMode.NONE);
      schema = container.getSchema();
      ScanBatch.this.schemaChanged = true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T addField(MaterializedField field, Class<T> clazz) throws SchemaChangeException {
      ValueVector v = TypeHelper.getNewVector(field, oContext.getAllocator());
      if(!clazz.isAssignableFrom(v.getClass())) throw new SchemaChangeException(String.format("The class that was provided %s does not correspond to the expected vector type of %s.", clazz.getSimpleName(), v.getClass().getSimpleName()));
      addField(v);
      return (T) v;
    }

  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return container.iterator();
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  public void cleanup(){
    container.clear();
    oContext.close();
  }

}
