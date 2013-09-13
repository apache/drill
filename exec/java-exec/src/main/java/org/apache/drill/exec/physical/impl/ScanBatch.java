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

import java.util.Iterator;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Maps;

/**
 * Record batch used for a particular scan. Operators against one or more
 */
public class ScanBatch implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanBatch.class);

  final Map<MaterializedField, ValueVector> fieldVectorMap = Maps.newHashMap();

  private final VectorContainer container = new VectorContainer();
  private int recordCount;
  private boolean schemaChanged = true;
  private final FragmentContext context;
  private Iterator<RecordReader> readers;
  private RecordReader currentReader;
  private BatchSchema schema;
  private final Mutator mutator = new Mutator();

  public ScanBatch(FragmentContext context, Iterator<RecordReader> readers) throws ExecutionSetupException {
    this.context = context;
    this.readers = readers;
    if (!readers.hasNext())
      throw new ExecutionSetupException("A scan batch must contain at least one reader.");
    this.currentReader = readers.next();
    this.currentReader.setup(mutator);
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
        currentReader.setup(mutator);
      } catch (ExecutionSetupException e) {
        this.context.fail(e);
        releaseAssets();
        return IterOutcome.STOP;
      }
    }

    if (schemaChanged) {
      schemaChanged = false;
      return IterOutcome.OK_NEW_SCHEMA;
    } else {
      return IterOutcome.OK;
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

  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return container.iterator();
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

}
