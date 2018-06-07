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
package org.apache.drill.exec.physical.impl.unnest;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.physical.config.UnnestPOP;
import org.apache.drill.exec.record.AbstractTableFunctionRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchMemoryManager;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

import java.util.List;

import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OK;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OK_NEW_SCHEMA;

// TODO - handle the case where a user tries to unnest a scalar, should just return the column as is
public class UnnestRecordBatch extends AbstractTableFunctionRecordBatch<UnnestPOP> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnnestRecordBatch.class);

  private Unnest unnest;
  private boolean hasRemainder = false; // set to true if there is data left over for the current row AND if we want
                                        // to keep processing it. Kill may be called by a limit in a subquery that
                                        // requires us to stop processing thecurrent row, but not stop processing
                                        // the data.
  // In some cases we need to return a predetermined state from a call to next. These are:
  // 1) Kill is called due to an error occurring in the processing of the query. IterOutcome should be NONE
  // 2) Kill is called by LIMIT to stop processing of the current row (This occurs when the LIMIT is part of a subquery
  //    between UNNEST and LATERAL. Iteroutcome should be EMIT
  // 3) Kill is called by LIMIT downstream from LATERAL. IterOutcome should be NONE
  private IterOutcome nextState = OK;
  private int remainderIndex = 0;
  private int recordCount;
  private MaterializedField unnestFieldMetadata;
  private final UnnestMemoryManager memoryManager;

  public enum Metric implements MetricDef {
    INPUT_BATCH_COUNT,
    AVG_INPUT_BATCH_BYTES,
    AVG_INPUT_ROW_BYTES,
    INPUT_RECORD_COUNT,
    OUTPUT_BATCH_COUNT,
    AVG_OUTPUT_BATCH_BYTES,
    AVG_OUTPUT_ROW_BYTES,
    OUTPUT_RECORD_COUNT;

    @Override
    public int metricId() {
      return ordinal();
    }
  }


  /**
   * Memory manager for Unnest. Estimates the batch size exactly like we do for Flatten.
   */
  private class UnnestMemoryManager extends RecordBatchMemoryManager {

    private UnnestMemoryManager(int outputBatchSize) {
      super(outputBatchSize);
    }

    @Override
    public void update() {
      // Get sizing information for the batch.
      setRecordBatchSizer(new RecordBatchSizer(incoming));

      final TypedFieldId typedFieldId = incoming.getValueVectorId(popConfig.getColumn());
      final MaterializedField field = incoming.getSchema().getColumn(typedFieldId.getFieldIds()[0]);

      // Get column size of unnest column.
      RecordBatchSizer.ColumnSize columnSize = getRecordBatchSizer().getColumn(field.getName());

      // Average rowWidth of single element in the unnest list.
      // subtract the offset vector size from column data size.
      final int avgRowWidthSingleUnnestEntry = RecordBatchSizer
          .safeDivide(columnSize.getTotalNetSize() - (getOffsetVectorWidth() * columnSize.getValueCount()), columnSize
              .getElementCount());

      // Average rowWidth of outgoing batch.
      final int avgOutgoingRowWidth = avgRowWidthSingleUnnestEntry;

      // Number of rows in outgoing batch
      final int outputBatchSize = getOutputBatchSize();
      // Number of rows in outgoing batch
      setOutputRowCount(outputBatchSize, avgOutgoingRowWidth);

      setOutgoingRowWidth(avgOutgoingRowWidth);

      // Limit to lower bound of total number of rows possible for this batch
      // i.e. all rows fit within memory budget.
      setOutputRowCount(Math.min(columnSize.getElementCount(), getOutputRowCount()));

      if (logger.isDebugEnabled()) {
        logger.debug("BATCH_STATS, incoming:\n {}", getRecordBatchSizer());
      }

      updateIncomingStats();
    }

  }


  public UnnestRecordBatch(UnnestPOP pop, FragmentContext context) throws OutOfMemoryException {
    super(pop, context);
    pop.addUnnestBatch(this);
    // get the output batch size from config.
    int configuredBatchSize = (int) context.getOptions().getOption(ExecConstants.OUTPUT_BATCH_SIZE_VALIDATOR);
    memoryManager = new UnnestMemoryManager(configuredBatchSize);
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  protected void killIncoming(boolean sendUpstream) {
    // Kill may be received from an operator downstream of the corresponding lateral, or from
    // a limit that is in a subqueruy between unnest and lateral. In the latter case, unnest has to handle the limit.
    // In the former case, Lateral will handle most of the kill handling.

    Preconditions.checkNotNull(lateral);
    // Do not call kill on incoming. Lateral Join has the responsibility for killing incoming
    if (context.getExecutorState().isFailed() || lateral.getLeftOutcome() == IterOutcome.STOP) {
      logger.debug("Kill received. Stopping all processing");
      nextState = IterOutcome.NONE ;
    } else {
      // if we have already processed the record, then kill from a limit has no meaning.
      // if, however, we have values remaining to be emitted, and limit has been reached,
      // we abandon the remainder and send an empty batch with EMIT.
      logger.debug("Kill received from subquery. Stopping processing of current input row.");
      if(hasRemainder) {
        nextState = IterOutcome.EMIT;
      }
    }
    hasRemainder = false; // whatever the case, we need to stop processing the current row.
  }

  @Override
  public IterOutcome innerNext() {

    Preconditions.checkNotNull(lateral);

    // Short circuit if record batch has already sent all data and is done
    if (state == BatchState.DONE) {
      return IterOutcome.NONE;
    }

    if (nextState == IterOutcome.NONE || nextState == IterOutcome.EMIT) {
      return nextState;
    }

    if (hasRemainder) {
      return doWork();
    }

    // We do not need to call next() unlike the other operators.
    // When unnest's innerNext is called, the LateralJoin would have already
    // updated the incoming vector.
    // We do, however, need to call doWork() to do the actual work.
    // We also need to handle schema build if it is the first batch

    if ((state == BatchState.FIRST)) {
      state = BatchState.NOT_FIRST;
      try {
        stats.startSetup();
        hasRemainder = true; // next call to next will handle the actual data.
        logger.debug("First batch received");
        schemaChanged(); // checks if schema has changed (redundant in this case becaause it has) AND saves the
                         // current field metadata for check in subsequent iterations
        setupNewSchema();
        stats.batchReceived(0, incoming.getRecordCount(), true);
      } catch (SchemaChangeException ex) {
        kill(false);
        logger.error("Failure during query", ex);
        context.getExecutorState().fail(ex);
        return IterOutcome.STOP;
      } finally {
        stats.stopSetup();
      }
      return IterOutcome.OK_NEW_SCHEMA;
    } else {
      container.zeroVectors();
      // Check if schema has changed
      if (lateral.getRecordIndex() == 0) {
        boolean isNewSchema = schemaChanged();
        stats.batchReceived(0, incoming.getRecordCount(), isNewSchema);
        if (isNewSchema) {
          hasRemainder = true;     // next call to next will handle the actual data.
          try {
            setupNewSchema();
          } catch (SchemaChangeException ex) {
            kill(false);
            logger.error("Failure during query", ex);
            context.getExecutorState().fail(ex);
            return IterOutcome.STOP;
          }
          return OK_NEW_SCHEMA;
        }
        // else
        unnest.resetGroupIndex();
      }
      return doWork();
    }

  }

    @Override
  public VectorContainer getOutgoingContainer() {
    return this.container;
  }

  @SuppressWarnings("resource")
  private void setUnnestVector() {
    final TypedFieldId typedFieldId = incoming.getValueVectorId(popConfig.getColumn());
    final MaterializedField field = incoming.getSchema().getColumn(typedFieldId.getFieldIds()[0]);
    final RepeatedValueVector vector;
    final ValueVector inVV =
        incoming.getValueAccessorById(field.getValueClass(), typedFieldId.getFieldIds()).getValueVector();

    if (!(inVV instanceof RepeatedValueVector)) {
      if (incoming.getRecordCount() != 0) {
        throw UserException.unsupportedError().message("Unnest does not support inputs of non-list values.")
            .build(logger);
      }
      // Inherited from FLATTEN. When does this happen???
      //when incoming recordCount is 0, don't throw exception since the type being seen here is not solid
      logger.error("setUnnestVector cast failed and recordcount is 0, create empty vector anyway.");
      vector = new RepeatedMapVector(field, oContext.getAllocator(), null);
    } else {
      vector = RepeatedValueVector.class.cast(inVV);
    }
    unnest.setUnnestField(vector);
  }

  protected IterOutcome doWork() {
    Preconditions.checkNotNull(lateral);
    memoryManager.update();
    unnest.setOutputCount(memoryManager.getOutputRowCount());
    final int incomingRecordCount = incoming.getRecordCount();
    final int currentRecord = lateral.getRecordIndex();
    // We call this in setupSchema, but we also need to call it here so we have a reference to the appropriate vector
    // inside of the the unnest for the current batch
    setUnnestVector();

    //Expected output count is the num of values in the unnest colum array for the current record
    final int childCount =
        incomingRecordCount == 0 ? 0 : unnest.getUnnestField().getAccessor().getInnerValueCountAt(currentRecord) - remainderIndex;

    // Unnest the data
    final int outputRecords = childCount == 0 ? 0 : unnest.unnestRecords(childCount);

    logger.debug("{} values out of {} were processed.", outputRecords, childCount);
    // Keep track of any spill over into another batch. Happens only if you artificially set the output batch
    // size for unnest to a low number
    if (outputRecords < childCount) {
      hasRemainder = true;
      remainderIndex += outputRecords;
      logger.debug("Output spilled into new batch. IterOutcome: OK.");
    } else {
      hasRemainder = false;
      remainderIndex = 0;
      logger.debug("IterOutcome: EMIT.");
    }
    this.recordCount = outputRecords;

    memoryManager.updateOutgoingStats(outputRecords);
    // If the current incoming record has spilled into two batches, we return
    // IterOutcome.OK so that the Lateral Join can keep calling next() until the
    // entire incoming recods has been unnested. If the entire records has been
    // unnested, we return EMIT and any blocking operators in the pipeline will
    // unblock.
    if (logger.isDebugEnabled()) {
      logger.debug("BATCH_STATS, outgoing:\n {}", new RecordBatchSizer(this));
    }
    return hasRemainder ? IterOutcome.OK : IterOutcome.EMIT;
  }

  /**
   * The data layout is the same for the actual data within a repeated field, as it is in a scalar vector for
   * the same sql type. For example, a repeated int vector has a vector of offsets into a regular int vector to
   * represent the lists. As the data layout for the actual values in the same in the repeated vector as in the
   * scalar vector of the same type, we can avoid making individual copies for the column being unnested, and just
   * use vector copies between the inner vector of the repeated field to the resulting scalar vector from the unnest
   * operation. This is completed after we determine how many records will fit (as we will hit either a batch end, or
   * the end of one of the other vectors while we are copying the data of the other vectors alongside each new unnested
   * value coming out of the repeated field.)
   */
  @SuppressWarnings("resource") private TransferPair getUnnestFieldTransferPair(FieldReference reference) {
    final TypedFieldId fieldId = incoming.getValueVectorId(popConfig.getColumn());
    final Class<?> vectorClass = incoming.getSchema().getColumn(fieldId.getFieldIds()[0]).getValueClass();
    final ValueVector unnestField = incoming.getValueAccessorById(vectorClass, fieldId.getFieldIds()).getValueVector();

    TransferPair tp = null;
    if (unnestField instanceof RepeatedMapVector) {
      tp = ((RepeatedMapVector) unnestField)
          .getTransferPairToSingleMap(reference.getAsNamePart().getName(), oContext.getAllocator());
    } else if (!(unnestField instanceof RepeatedValueVector)) {
      if (incoming.getRecordCount() != 0) {
        throw UserException.unsupportedError().message("Unnest does not support inputs of non-list values.")
            .build(logger);
      }
      logger.error("Cannot cast {} to RepeatedValueVector", unnestField);
      //when incoming recordCount is 0, don't throw exception since the type being seen here is not solid
      final ValueVector vv = new RepeatedMapVector(unnestField.getField(), oContext.getAllocator(), null);
      tp = RepeatedValueVector.class.cast(vv)
          .getTransferPair(reference.getAsNamePart().getName(), oContext.getAllocator());
    } else {
      final ValueVector vvIn = RepeatedValueVector.class.cast(unnestField).getDataVector();
      // vvIn may be null because of fast schema return for repeated list vectors
      if (vvIn != null) {
        tp = vvIn.getTransferPair(reference.getAsNamePart().getName(), oContext.getAllocator());
      }
    }
    return tp;
  }

  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    Preconditions.checkNotNull(lateral);
    container.clear();
    recordCount = 0;
    final List<TransferPair> transfers = Lists.newArrayList();

    final FieldReference fieldReference = new FieldReference(popConfig.getColumn());

    final TransferPair transferPair = getUnnestFieldTransferPair(fieldReference);

    final ValueVector unnestVector = transferPair.getTo();
    transfers.add(transferPair);
    container.add(unnestVector);
    logger.debug("Added transfer for unnest expression.");
    container.buildSchema(SelectionVectorMode.NONE);

    this.unnest = new UnnestImpl();
    unnest.setup(context, incoming, this, transfers, lateral);
    setUnnestVector();
    return true;
  }

  /**
   * Compares the schema of the unnest column in the current incoming with the schema of
   * the unnest column in the previous incoming.
   * Also saves the schema for comparison in future iterations
   *
   * @return true if the schema has changed, false otherwise
   */
  private boolean schemaChanged() {
    final TypedFieldId fieldId = incoming.getValueVectorId(popConfig.getColumn());
    final MaterializedField thisField = incoming.getSchema().getColumn(fieldId.getFieldIds()[0]);
    final MaterializedField prevField = unnestFieldMetadata;
    Preconditions.checkNotNull(thisField);

    // isEquivalent may return false if the order of the fields has changed. This usually does not
    // happen but if it does we end up throwing a spurious schema change exeption
    if (prevField == null || !prevField.isEquivalent(thisField)) {
      logger.debug("Schema changed");
      // We should store the clone of MaterializedField for unnest column instead of reference. When the column is of
      // type Map and there is change in any children field of the Map then that will update the reference variable and
      // isEquivalent check will still return true.
      unnestFieldMetadata = thisField.clone();
      return true;
    }
    return false;
  }

  private void updateStats() {
    if(memoryManager.getRecordBatchSizer() == null) {
      return;
    }
    stats.setLongStat(Metric.INPUT_BATCH_COUNT, memoryManager.getNumIncomingBatches());
    stats.setLongStat(Metric.AVG_INPUT_BATCH_BYTES, memoryManager.getAvgInputBatchSize());
    stats.setLongStat(Metric.AVG_INPUT_ROW_BYTES, memoryManager.getAvgInputRowWidth());
    stats.setLongStat(Metric.INPUT_RECORD_COUNT, memoryManager.getTotalInputRecords());
    stats.setLongStat(Metric.OUTPUT_BATCH_COUNT, memoryManager.getNumOutgoingBatches());
    stats.setLongStat(Metric.AVG_OUTPUT_BATCH_BYTES, memoryManager.getAvgOutputBatchSize());
    stats.setLongStat(Metric.AVG_OUTPUT_ROW_BYTES, memoryManager.getAvgOutputRowWidth());
    stats.setLongStat(Metric.OUTPUT_RECORD_COUNT, memoryManager.getTotalOutputRecords());

    logger.debug("BATCH_STATS, incoming aggregate: batch count : {}, avg batch bytes : {},  avg row bytes : {}, record count : {}",
        memoryManager.getNumIncomingBatches(), memoryManager.getAvgInputBatchSize(),
        memoryManager.getAvgInputRowWidth(), memoryManager.getTotalInputRecords());

    logger.debug("BATCH_STATS, outgoing aggregate: batch count : {}, avg batch bytes : {},  avg row bytes : {}, record count : {}",
        memoryManager.getNumOutgoingBatches(), memoryManager.getAvgOutputBatchSize(),
        memoryManager.getAvgOutputRowWidth(), memoryManager.getTotalOutputRecords());

  }

  @Override
  public void close() {
    updateStats();
    super.close();
  }

}
