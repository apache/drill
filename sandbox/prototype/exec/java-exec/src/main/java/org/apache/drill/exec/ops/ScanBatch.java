/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.ops;

import java.util.Iterator;

import org.apache.drill.exec.exception.ExecutionSetupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.InvalidValueAccessor;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.vector.ValueVector;
import org.apache.drill.exec.store.RecordReader;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.procedures.IntObjectProcedure;

/**
 * Record batch used for a particular scan. Operators against one or more
 */
public abstract class ScanBatch implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanBatch.class);

  private IntObjectOpenHashMap<ValueVector<?>> fields = new IntObjectOpenHashMap<ValueVector<?>>();
  private BatchSchema schema;
  private int recordCount;
  private boolean schemaChanged = true;
  private final FragmentContext context;
  private Iterator<RecordReader> readers;
  private RecordReader currentReader;
  private final BatchSchema expectedSchema;
  private final Mutator mutator = new Mutator();

  public ScanBatch(BatchSchema expectedSchema, Iterator<RecordReader> readers, FragmentContext context)
      throws ExecutionSetupException {
    this.expectedSchema = expectedSchema;
    this.context = context;
    this.readers = readers;
    if (!readers.hasNext()) throw new ExecutionSetupException("A scan batch must contain at least one reader.");
    this.currentReader = readers.next();
    this.currentReader.setup(expectedSchema, mutator);
  }

  private void schemaChanged() {
    schema = null;
    schemaChanged = true;
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
    fields.forEach(new IntObjectProcedure<ValueVector<?>>() {
      @Override
      public void apply(int key, ValueVector<?> value) {
        value.close();
      }
    });
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends ValueVector<T>> T getValueVector(int fieldId, Class<T> clazz) throws InvalidValueAccessor {
    if (fields.containsKey(fieldId))
      throw new InvalidValueAccessor(String.format("Unknown value accesor for field id %d."));
    ValueVector<?> vector = this.fields.lget();
    if (vector.getClass().isAssignableFrom(clazz)) {
      return (T) vector;
    } else {
      throw new InvalidValueAccessor(String.format(
          "You requested a field accessor of type %s for field id %d but the actual type was %s.",
          clazz.getCanonicalName(), fieldId, vector.getClass().getCanonicalName()));
    }
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
        currentReader.setup(expectedSchema, mutator);
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

  private class Mutator implements OutputMutator {

    public void removeField(int fieldId) throws SchemaChangeException {
      schemaChanged();
      ValueVector<?> v = fields.remove(fieldId);
      if (v == null) throw new SchemaChangeException("Failure attempting to remove an unknown field.");
      v.close();
    }

    public void addField(int fieldId, ValueVector<?> vector) {
      schemaChanged();
      ValueVector<?> v = fields.put(fieldId, vector);
      if (v != null) v.close();
    }

    @Override
    public void setNewSchema(BatchSchema schema) {
      ScanBatch.this.schema = schema;
      ScanBatch.this.schemaChanged = true;
    }

  }
}
