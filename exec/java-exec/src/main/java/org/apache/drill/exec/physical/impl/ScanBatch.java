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

import io.netty.buffer.DrillBuf;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
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
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Record batch used for a particular scan. Operators against one or more
 */
public class ScanBatch implements RecordBatch {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanBatch.class);

  private static final int MAX_RECORD_CNT = Character.MAX_VALUE;

  private final Map<MaterializedField.Key, ValueVector> fieldVectorMap = Maps.newHashMap();

  private final VectorContainer container = new VectorContainer();
  private int recordCount;
  private final FragmentContext context;
  private final OperatorContext oContext;
  private Iterator<RecordReader> readers;
  private RecordReader currentReader;
  private BatchSchema schema;
  private final Mutator mutator = new Mutator();
  private Iterator<String[]> partitionColumns;
  private String[] partitionValues;
  private List<ValueVector> partitionVectors;
  private List<Integer> selectedPartitionColumns;
  private String partitionColumnDesignator;
  private boolean first = true;
  private boolean done = false;

  public ScanBatch(PhysicalOperator subScanConfig, FragmentContext context, Iterator<RecordReader> readers, List<String[]> partitionColumns, List<Integer> selectedPartitionColumns) throws ExecutionSetupException {
    this.context = context;
    this.readers = readers;
    if (!readers.hasNext()) {
      throw new ExecutionSetupException("A scan batch must contain at least one reader.");
    }
    this.currentReader = readers.next();
    this.oContext = new OperatorContext(subScanConfig, context);
    this.currentReader.setOperatorContext(this.oContext);
    this.currentReader.setup(mutator);
    this.partitionColumns = partitionColumns.iterator();
    this.partitionValues = this.partitionColumns.hasNext() ? this.partitionColumns.next() : null;
    this.selectedPartitionColumns = selectedPartitionColumns;
    DrillConfig config = context.getConfig();
    this.partitionColumnDesignator = config == null ? "dir" : config.getString(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL);
    addPartitionVectors();
  }

  public ScanBatch(PhysicalOperator subScanConfig, FragmentContext context, Iterator<RecordReader> readers) throws ExecutionSetupException {
    this(subScanConfig, context, readers, Collections.<String[]> emptyList(), Collections.<Integer> emptyList());
  }

  public FragmentContext getContext() {
    return context;
  }

  public OperatorContext getOperatorContext() {
    return oContext;
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
  public void kill(boolean sendUpstream) {
    if (currentReader != null) {
      currentReader.cleanup();
    }

    if (sendUpstream) {
      done = true;
    } else {
      releaseAssets();
    }
  }

  private void releaseAssets() {
    container.zeroVectors();
  }

  @Override
  public IterOutcome next() {
    if (done) {
      return IterOutcome.NONE;
    }
    long t1 = System.nanoTime();
    oContext.getStats().startProcessing();
    try {
      try {
        currentReader.allocate(fieldVectorMap);
      } catch (OutOfMemoryException e) {
        logger.debug("Caught OutOfMemoryException");
        for (ValueVector v : fieldVectorMap.values()) {
          v.clear();
        }
        return IterOutcome.OUT_OF_MEMORY;
      }
      while ((recordCount = currentReader.next()) == 0) {
        try {
          if (!readers.hasNext()) {
            currentReader.cleanup();
            if (first) {
              first = false;
              done = true;
              populatePartitionVectors();
              container.buildSchema(SelectionVectorMode.NONE);
              schema = container.getSchema();
              return IterOutcome.OK_NEW_SCHEMA;
            }
            releaseAssets();
            return IterOutcome.NONE;
          }
          oContext.getStats().startSetup();
          try {
            currentReader.cleanup();
            currentReader = readers.next();
            partitionValues = partitionColumns.hasNext() ? partitionColumns.next() : null;
            currentReader.setup(mutator);
            try {
              currentReader.allocate(fieldVectorMap);
            } catch (OutOfMemoryException e) {
              logger.debug("Caught OutOfMemoryException");
              for (ValueVector v : fieldVectorMap.values()) {
                v.clear();
              }
              return IterOutcome.OUT_OF_MEMORY;
            }
            addPartitionVectors();
          } finally {
            oContext.getStats().stopSetup();
          }
        } catch (ExecutionSetupException e) {
          this.context.fail(e);
          releaseAssets();
          return IterOutcome.STOP;
        }
      }
      first = false;

      populatePartitionVectors();
      if (mutator.isNewSchema()) {
        container.buildSchema(SelectionVectorMode.NONE);
        schema = container.getSchema();
        long t2 = System.nanoTime();
//        System.out.println((t2 - t1) / recordCount);
//        BatchPrinter.printBatch(this, "\t");
        return IterOutcome.OK_NEW_SCHEMA;
      } else {
        long t2 = System.nanoTime();
//        System.out.println((t2 - t1) / recordCount);
//        BatchPrinter.printBatch(this, "\t");
        return IterOutcome.OK;
      }
    } catch (Exception ex) {
      logger.debug("Failed to read the batch. Stopping...", ex);
      context.fail(ex);
      return IterOutcome.STOP;
    } finally {
      oContext.getStats().stopProcessing();
    }
  }

  private void addPartitionVectors() throws ExecutionSetupException{
    try {
      if (partitionVectors != null) {
        for (ValueVector v : partitionVectors) {
          v.clear();
        }
      }
      partitionVectors = Lists.newArrayList();
      for (int i : selectedPartitionColumns) {
        MaterializedField field = MaterializedField.create(SchemaPath.getSimplePath(partitionColumnDesignator + i), Types.optional(MinorType.VARCHAR));
        ValueVector v = mutator.addField(field, NullableVarCharVector.class);
        partitionVectors.add(v);
      }
    } catch(SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  private void populatePartitionVectors() {
    for (int index = 0; index < selectedPartitionColumns.size(); index++) {
      int i = selectedPartitionColumns.get(index);
      NullableVarCharVector v = (NullableVarCharVector) partitionVectors.get(index);
      if (partitionValues.length > i) {
        String val = partitionValues[i];
        AllocationHelper.allocate(v, recordCount, val.length());
        byte[] bytes = val.getBytes();
        for (int j = 0; j < recordCount; j++) {
          v.getMutator().setSafe(j, bytes, 0, bytes.length);
        }
        v.getMutator().setValueCount(recordCount);
      } else {
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
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    return container.getValueAccessorById(clazz, ids);
  }



  private class Mutator implements OutputMutator {

    boolean schemaChange = true;

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T addField(MaterializedField field, Class<T> clazz) throws SchemaChangeException {
      // Check if the field exists
      ValueVector v = fieldVectorMap.get(field.key());

      if (v == null || v.getClass() != clazz) {
        // Field does not exist add it to the map and the output container
        v = TypeHelper.getNewVector(field, oContext.getAllocator());
        if (!clazz.isAssignableFrom(v.getClass())) {
          throw new SchemaChangeException(String.format("The class that was provided %s does not correspond to the expected vector type of %s.", clazz.getSimpleName(), v.getClass().getSimpleName()));
        }
        container.add(v);
        fieldVectorMap.put(field.key(), v);

        // Adding new vectors to the container mark that the schema has changed
        schemaChange = true;
      }

      return (T) v;
    }

    @Override
    public void addFields(List<ValueVector> vvList) {
      for (ValueVector v : vvList) {
        fieldVectorMap.put(v.getField().key(), v);
        container.add(v);
      }
      schemaChange = true;
    }

    @Override
    public void allocate(int recordCount) {
      for (ValueVector v : fieldVectorMap.values()) {
        AllocationHelper.allocate(v, recordCount, 50, 10);
      }
    }

    @Override
    public boolean isNewSchema() {
      if (schemaChange == true) {
        schemaChange = false;
        return true;
      }
      return false;
    }

    @Override
    public DrillBuf getManagedBuffer() {
      return oContext.getManagedBuffer();
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

  public void cleanup() {
    container.clear();
    for (ValueVector v : partitionVectors) {
      v.clear();
    }
    fieldVectorMap.clear();
    currentReader.cleanup();
    oContext.close();
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    throw new UnsupportedOperationException(String.format(" You should not call getOutgoingContainer() for class %s", this.getClass().getCanonicalName()));
  }

}
