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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Maps;

/**
 * Record batch used for a particular scan. Operators against one or more
 */
public class ScanBatch implements CloseableRecordBatch {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanBatch.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(ScanBatch.class);

  /** Main collection of fields' value vectors. */
  private final VectorContainer container = new VectorContainer();

  /** Fields' value vectors indexed by fields' keys. */
  private final Map<String, ValueVector> fieldVectorMap =
      Maps.newHashMap();

  private int recordCount;
  private final FragmentContext context;
  private final OperatorContext oContext;
  private Iterator<RecordReader> readers;
  private RecordReader currentReader;
  private BatchSchema schema;
  private final Mutator mutator = new Mutator();
  private boolean done = false;
  private SchemaChangeCallBack callBack = new SchemaChangeCallBack();
  private boolean hasReadNonEmptyFile = false;
  private Map<String, ValueVector> implicitVectors;
  private Iterator<Map<String, String>> implicitColumns;
  private Map<String, String> implicitValues;

  public ScanBatch(PhysicalOperator subScanConfig, FragmentContext context,
                   OperatorContext oContext, Iterator<RecordReader> readers,
                   List<Map<String, String>> implicitColumns) throws ExecutionSetupException {
    this.context = context;
    this.readers = readers;
    if (!readers.hasNext()) {
      throw new ExecutionSetupException("A scan batch must contain at least one reader.");
    }
    currentReader = readers.next();
    this.oContext = oContext;

    boolean setup = false;
    try {
      oContext.getStats().startProcessing();
      currentReader.setup(oContext, mutator);
      setup = true;
    } finally {
      // if we had an exception during setup, make sure to release existing data.
      if (!setup) {
        try {
          currentReader.close();
        } catch(final Exception e) {
          throw new ExecutionSetupException(e);
        }
      }
      oContext.getStats().stopProcessing();
    }
    this.implicitColumns = implicitColumns.iterator();
    this.implicitValues = this.implicitColumns.hasNext() ? this.implicitColumns.next() : null;

    addImplicitVectors();
  }

  public ScanBatch(PhysicalOperator subScanConfig, FragmentContext context,
                   Iterator<RecordReader> readers)
      throws ExecutionSetupException {
    this(subScanConfig, context,
        context.newOperatorContext(subScanConfig),
        readers, Collections.<Map<String, String>> emptyList());
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
  public void kill(boolean sendUpstream) {
    if (sendUpstream) {
      done = true;
    } else {
      releaseAssets();
    }
  }

  private void releaseAssets() {
    container.zeroVectors();
  }

  private void clearFieldVectorMap() {
    for (final ValueVector v : fieldVectorMap.values()) {
      v.clear();
    }
  }

  @Override
  public IterOutcome next() {
    if (done) {
      return IterOutcome.NONE;
    }
    oContext.getStats().startProcessing();
    try {
      try {
        injector.injectChecked(context.getExecutionControls(), "next-allocate", OutOfMemoryException.class);

        currentReader.allocate(fieldVectorMap);
      } catch (OutOfMemoryException e) {
        logger.debug("Caught Out of Memory Exception", e);
        clearFieldVectorMap();
        return IterOutcome.OUT_OF_MEMORY;
      }
      while ((recordCount = currentReader.next()) == 0) {
        try {
          if (!readers.hasNext()) {
            // We're on the last reader, and it has no (more) rows.
            currentReader.close();
            releaseAssets();
            done = true;  // have any future call to next() return NONE

            if (mutator.isNewSchema()) {
              // This last reader has a new schema (e.g., we have a zero-row
              // file or other source).  (Note that some sources have a non-
              // null/non-trivial schema even when there are no rows.)

              container.buildSchema(SelectionVectorMode.NONE);
              schema = container.getSchema();

              return IterOutcome.OK_NEW_SCHEMA;
            }
            return IterOutcome.NONE;
          }
          // At this point, the reader that hit its end is not the last reader.

          // If all the files we have read so far are just empty, the schema is not useful
          if (! hasReadNonEmptyFile) {
            container.clear();
            for (ValueVector v : fieldVectorMap.values()) {
              v.clear();
            }
            fieldVectorMap.clear();
          }

          currentReader.close();
          currentReader = readers.next();
          implicitValues = implicitColumns.hasNext() ? implicitColumns.next() : null;
          currentReader.setup(oContext, mutator);
          try {
            currentReader.allocate(fieldVectorMap);
          } catch (OutOfMemoryException e) {
            logger.debug("Caught OutOfMemoryException");
            clearFieldVectorMap();
            return IterOutcome.OUT_OF_MEMORY;
          }
          addImplicitVectors();
        } catch (ExecutionSetupException e) {
          this.context.fail(e);
          releaseAssets();
          return IterOutcome.STOP;
        }
      }
      // At this point, the current reader has read 1 or more rows.

      hasReadNonEmptyFile = true;
      populateImplicitVectors();

      for (VectorWrapper w : container) {
        w.getValueVector().getMutator().setValueCount(recordCount);
      }


      // this is a slight misuse of this metric but it will allow Readers to report how many records they generated.
      final boolean isNewSchema = mutator.isNewSchema();
      oContext.getStats().batchReceived(0, getRecordCount(), isNewSchema);

      if (isNewSchema) {
        container.buildSchema(SelectionVectorMode.NONE);
        schema = container.getSchema();
        return IterOutcome.OK_NEW_SCHEMA;
      } else {
        return IterOutcome.OK;
      }
    } catch (OutOfMemoryException ex) {
      context.fail(UserException.memoryError(ex).build(logger));
      return IterOutcome.STOP;
    } catch (Exception ex) {
      logger.debug("Failed to read the batch. Stopping...", ex);
      context.fail(ex);
      return IterOutcome.STOP;
    } finally {
      oContext.getStats().stopProcessing();
    }
  }

  private void addImplicitVectors() throws ExecutionSetupException {
    try {
      if (implicitVectors != null) {
        for (ValueVector v : implicitVectors.values()) {
          v.clear();
        }
      }
      implicitVectors = Maps.newHashMap();

      if (implicitValues != null) {
        for (String column : implicitValues.keySet()) {
          final MaterializedField field = MaterializedField.create(column, Types.optional(MinorType.VARCHAR));
          final ValueVector v = mutator.addField(field, NullableVarCharVector.class);
          implicitVectors.put(column, v);
        }
      }
    } catch(SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  private void populateImplicitVectors() {
    if (implicitValues != null) {
      for (Map.Entry<String, String> entry : implicitValues.entrySet()) {
        final NullableVarCharVector v = (NullableVarCharVector) implicitVectors.get(entry.getKey());
        String val;
        if ((val = entry.getValue()) != null) {
          AllocationHelper.allocate(v, recordCount, val.length());
          final byte[] bytes = val.getBytes();
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
    /** Whether schema has changed since last inquiry (via #isNewSchema}).  Is
     *  true before first inquiry. */
    private boolean schemaChanged = true;


    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T addField(MaterializedField field,
                                              Class<T> clazz) throws SchemaChangeException {
      // Check if the field exists.
      ValueVector v = fieldVectorMap.get(field.getPath());
      if (v == null || v.getClass() != clazz) {
        // Field does not exist--add it to the map and the output container.
        v = TypeHelper.getNewVector(field, oContext.getAllocator(), callBack);
        if (!clazz.isAssignableFrom(v.getClass())) {
          throw new SchemaChangeException(
              String.format(
                  "The class that was provided, %s, does not correspond to the "
                  + "expected vector type of %s.",
                  clazz.getSimpleName(), v.getClass().getSimpleName()));
        }

        final ValueVector old = fieldVectorMap.put(field.getPath(), v);
        if (old != null) {
          old.clear();
          container.remove(old);
        }

        container.add(v);
        // Added new vectors to the container--mark that the schema has changed.
        schemaChanged = true;
      }

      return clazz.cast(v);
    }

    @Override
    public void allocate(int recordCount) {
      for (final ValueVector v : fieldVectorMap.values()) {
        AllocationHelper.allocate(v, recordCount, 50, 10);
      }
    }

    /**
     * Reports whether schema has changed (field was added or re-added) since
     * last call to {@link #isNewSchema}.  Returns true at first call.
     */
    @Override
    public boolean isNewSchema() {
      // Check if top-level schema or any of the deeper map schemas has changed.

      // Note:  Callback's getSchemaChangedAndReset() must get called in order
      // to reset it and avoid false reports of schema changes in future.  (Be
      // careful with short-circuit OR (||) operator.)

      final boolean deeperSchemaChanged = callBack.getSchemaChangedAndReset();
      if (schemaChanged || deeperSchemaChanged) {
        schemaChanged = false;
        return true;
      }
      return false;
    }

    @Override
    public DrillBuf getManagedBuffer() {
      return oContext.getManagedBuffer();
    }

    @Override
    public CallBack getCallBack() {
      return callBack;
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

  @Override
  public void close() throws Exception {
    container.clear();
    for (final ValueVector v : implicitVectors.values()) {
      v.clear();
    }
    fieldVectorMap.clear();
    currentReader.close();
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    throw new UnsupportedOperationException(
        String.format("You should not call getOutgoingContainer() for class %s",
                      this.getClass().getCanonicalName()));
  }
}
