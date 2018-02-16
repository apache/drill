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
package org.apache.drill.exec.physical.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Record batch used for a particular scan. Operators against one or more
 */
public class ScanBatch implements CloseableRecordBatch {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanBatch.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(ScanBatch.class);

  /** Main collection of fields' value vectors. */
  private final VectorContainer container = new VectorContainer();

  private int recordCount;
  private final FragmentContext context;
  private final OperatorContext oContext;
  private Iterator<RecordReader> readers;
  private RecordReader currentReader;
  private BatchSchema schema;
  private final Mutator mutator;
  private boolean done = false;
  private Iterator<Map<String, String>> implicitColumns;
  private Map<String, String> implicitValues;
  private final BufferAllocator allocator;
  private final List<Map<String, String>> implicitColumnList;
  private String currentReaderClassName;
  /**
   *
   * @param context
   * @param oContext
   * @param readerList
   * @param implicitColumnList : either an emptylist when all the readers do not have implicit
   *                        columns, or there is a one-to-one mapping between reader and implicitColumns.
   */
  public ScanBatch(FragmentContext context,
                   OperatorContext oContext, List<RecordReader> readerList,
                   List<Map<String, String>> implicitColumnList) {
    this.context = context;
    this.readers = readerList.iterator();
    this.implicitColumns = implicitColumnList.iterator();
    if (!readers.hasNext()) {
      throw UserException.internalError(
          new ExecutionSetupException("A scan batch must contain at least one reader."))
        .build(logger);
    }

    this.oContext = oContext;
    allocator = oContext.getAllocator();
    mutator = new Mutator(oContext, allocator, container);

    oContext.getStats().startProcessing();
    try {
      if (!verifyImplcitColumns(readerList.size(), implicitColumnList)) {
        Exception ex = new ExecutionSetupException("Either implicit column list does not have same cardinality as reader list, "
            + "or implicit columns are not same across all the record readers!");
        throw UserException.internalError(ex)
            .addContext("Setup failed for", readerList.get(0).getClass().getSimpleName())
            .build(logger);
      }

      this.implicitColumnList = implicitColumnList;
      addImplicitVectors();
      currentReader = null;
    } finally {
      oContext.getStats().stopProcessing();
    }
  }

  public ScanBatch(PhysicalOperator subScanConfig, FragmentContext context,
                   List<RecordReader> readers)
      throws ExecutionSetupException {
    this(context, context.newOperatorContext(subScanConfig),
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

  @Override
  public IterOutcome next() {
    if (done) {
      return IterOutcome.NONE;
    }
    oContext.getStats().startProcessing();
    try {
      while (true) {
        if (currentReader == null && !getNextReaderIfHas()) {
          releaseAssets(); // All data has been read. Release resource.
          done = true;
          return IterOutcome.NONE;
        }
        injector.injectChecked(context.getExecutionControls(), "next-allocate", OutOfMemoryException.class);
        currentReader.allocate(mutator.fieldVectorMap());

        recordCount = currentReader.next();
        Preconditions.checkArgument(recordCount >= 0, "recordCount from RecordReader.next() should not be negative");
        boolean isNewSchema = mutator.isNewSchema();
        populateImplicitVectorsAndSetCount();
        oContext.getStats().batchReceived(0, recordCount, isNewSchema);

        if (recordCount == 0) {
          currentReader.close();
          currentReader = null; // indicate currentReader is complete,
                                // and fetch next reader in next loop iterator if required.
        }

        if (isNewSchema) {
          // Even when recordCount = 0, we should return return OK_NEW_SCHEMA if current reader presents a new schema.
          // This could happen when data sources have a non-trivial schema with 0 row.
          container.buildSchema(SelectionVectorMode.NONE);
          schema = container.getSchema();
          return IterOutcome.OK_NEW_SCHEMA;
        }

        // Handle case of same schema.
        if (recordCount == 0) {
            continue; // Skip to next loop iteration if reader returns 0 row and has same schema.
        } else {
          // return OK if recordCount > 0 && ! isNewSchema
          return IterOutcome.OK;
        }
      }
    } catch (OutOfMemoryException ex) {
      clearFieldVectorMap();
      throw UserException.memoryError(ex).build(logger);
    } catch (ExecutionSetupException e) {
      if (currentReader != null) {
        try {
          currentReader.close();
        } catch (final Exception e2) {
          logger.error("Close failed for reader " + currentReaderClassName, e2);
        }
      }
      throw UserException.internalError(e)
          .addContext("Setup failed for", currentReaderClassName)
          .build(logger);
    } catch (UserException ex) {
      throw ex;
    } catch (Exception ex) {
      throw UserException.internalError(ex).build(logger);
    } finally {
      oContext.getStats().stopProcessing();
    }
  }

  private void releaseAssets() {
    container.zeroVectors();
  }

  private void clearFieldVectorMap() {
    for (final ValueVector v : mutator.fieldVectorMap().values()) {
      v.clear();
    }
    for (final ValueVector v : mutator.implicitFieldVectorMap.values()) {
      v.clear();
    }
  }

  private boolean getNextReaderIfHas() throws ExecutionSetupException {
    if (!readers.hasNext()) {
      return false;
    }
    currentReader = readers.next();
    if (readers.hasNext()) {
      readers.remove();
    }
    implicitValues = implicitColumns.hasNext() ? implicitColumns.next() : null;
    currentReader.setup(oContext, mutator);
    currentReaderClassName = currentReader.getClass().getSimpleName();
    return true;
  }

  private void addImplicitVectors() {
    try {
      if (!implicitColumnList.isEmpty()) {
        for (String column : implicitColumnList.get(0).keySet()) {
          final MaterializedField field = MaterializedField.create(column, Types.optional(MinorType.VARCHAR));
          mutator.addField(field, NullableVarCharVector.class, true /*implicit field*/);
        }
      }
    } catch(SchemaChangeException e) {
      // No exception should be thrown here.
      throw UserException.internalError(e)
        .addContext("Failure while allocating implicit vectors")
        .build(logger);
    }
  }

  private void populateImplicitVectorsAndSetCount() {
    mutator.populateImplicitVectors(implicitValues, recordCount);
    for (Map.Entry<String, ValueVector> entry: mutator.fieldVectorMap().entrySet()) {
      logger.debug("set record count {} for vv {}", recordCount, entry.getKey());
      entry.getValue().getMutator().setValueCount(recordCount);
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

  /**
   * Row set mutator implementation provided to record readers created by
   * this scan batch. Made visible so that tests can create this mutator
   * without also needing a ScanBatch instance. (This class is really independent
   * of the ScanBatch, but resides here for historical reasons. This is,
   * in turn, the only use of the generated vector readers in the vector
   * package.)
   */

  @VisibleForTesting
  public static class Mutator implements OutputMutator {
    /** Flag keeping track whether top-level schema has changed since last inquiry (via #isNewSchema}).
     * It's initialized to false, or reset to false after #isNewSchema or after #clear, until a new value vector
     * or a value vector with different type is added to fieldVectorMap.
     **/
    private boolean schemaChanged;

    /** Regular fields' value vectors indexed by fields' keys. */
    private final CaseInsensitiveMap<ValueVector> regularFieldVectorMap =
            CaseInsensitiveMap.newHashMap();

    /** Implicit fields' value vectors index by fields' keys. */
    private final CaseInsensitiveMap<ValueVector> implicitFieldVectorMap =
        CaseInsensitiveMap.newHashMap();

    private final SchemaChangeCallBack callBack = new SchemaChangeCallBack();
    private final BufferAllocator allocator;

    private final VectorContainer container;

    private final OperatorContext oContext;

    public Mutator(OperatorContext oContext, BufferAllocator allocator, VectorContainer container) {
      this.oContext = oContext;
      this.allocator = allocator;
      this.container = container;
      this.schemaChanged = false;
    }

    public Map<String, ValueVector> fieldVectorMap() {
      return regularFieldVectorMap;
    }

    public Map<String, ValueVector> implicitFieldVectorMap() {
      return implicitFieldVectorMap;
    }

    @Override
    public <T extends ValueVector> T addField(MaterializedField field,
                                              Class<T> clazz) throws SchemaChangeException {
      return addField(field, clazz, false);
    }

    @Override
    public void allocate(int recordCount) {
      for (final ValueVector v : regularFieldVectorMap.values()) {
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

    public void clear() {
      regularFieldVectorMap.clear();
      implicitFieldVectorMap.clear();
      schemaChanged = false;
    }

    @SuppressWarnings("resource")
    private <T extends ValueVector> T addField(MaterializedField field,
        Class<T> clazz, boolean isImplicitField) throws SchemaChangeException {
      Map<String, ValueVector> fieldVectorMap;

      if (isImplicitField) {
        fieldVectorMap = implicitFieldVectorMap;
      } else {
        fieldVectorMap = regularFieldVectorMap;
      }

      if (!isImplicitField && implicitFieldVectorMap.containsKey(field.getName()) ||
          isImplicitField && regularFieldVectorMap.containsKey(field.getName())) {
        throw new SchemaChangeException(
            String.format(
                "It's not allowed to have regular field and implicit field share common name %s. "
                    + "Either change regular field name in datasource, or change the default implicit field names.",
                field.getName()));
      }

      // Check if the field exists.
      ValueVector v = fieldVectorMap.get(field.getName());
      if (v == null || v.getClass() != clazz) {
        // Field does not exist--add it to the map and the output container.
        v = TypeHelper.getNewVector(field, allocator, callBack);
        if (!clazz.isAssignableFrom(v.getClass())) {
          throw new SchemaChangeException(
              String.format(
                  "The class that was provided, %s, does not correspond to the "
                      + "expected vector type of %s.",
                  clazz.getSimpleName(), v.getClass().getSimpleName()));
        }

        final ValueVector old = fieldVectorMap.put(field.getName(), v);
        if (old != null) {
          old.clear();
          container.remove(old);
        }

        container.add(v);
        // Only mark schema change for regular vectors added to the container; implicit schema is constant.
        if (!isImplicitField) {
          schemaChanged = true;
        }
      }

      return clazz.cast(v);
    }

    private void populateImplicitVectors(Map<String, String> implicitValues, int recordCount) {
      if (implicitValues != null) {
        for (Map.Entry<String, String> entry : implicitValues.entrySet()) {
          @SuppressWarnings("resource")
          final NullableVarCharVector v = (NullableVarCharVector) implicitFieldVectorMap.get(entry.getKey());
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
    mutator.clear();
    if (currentReader != null) {
      currentReader.close();
    }
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    throw new UnsupportedOperationException(
        String.format("You should not call getOutgoingContainer() for class %s",
                      this.getClass().getCanonicalName()));
  }

  /**
   * Verify list of implicit column values is valid input:
   *   - Either implicit column list is empty;
   *   - Or implicit column list has same sie as reader list, and the key set is same across all the readers.
   * @param numReaders
   * @param implicitColumnList
   * @return return true if
   */
  private boolean verifyImplcitColumns(int numReaders, List<Map<String, String>> implicitColumnList) {
    if (implicitColumnList.isEmpty()) {
      return true;
    }

    if (numReaders != implicitColumnList.size()) {
      return false;
    }

    Map<String, String> firstMap = implicitColumnList.get(0);

    for (int i = 1; i< implicitColumnList.size(); i++) {
      Map<String, String> nonFirstMap = implicitColumnList.get(i);

      if (!firstMap.keySet().equals(nonFirstMap.keySet())) {
        return false;
      }
    }

    return true;
  }
}
