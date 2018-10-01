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

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
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
import org.apache.drill.exec.util.record.RecordBatchStats;
import org.apache.drill.exec.util.record.RecordBatchStats.RecordBatchIOType;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

import java.util.List;

import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OK_NEW_SCHEMA;

// TODO - handle the case where a user tries to unnest a scalar, should just return the column as is
public class UnnestRecordBatch extends AbstractTableFunctionRecordBatch<UnnestPOP> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnnestRecordBatch.class);

  private final String rowIdColumnName; // name of the field holding the rowId implicit column
  private IntVector rowIdVector; // vector to keep the implicit rowId column in

  private Unnest unnest = new UnnestImpl();
  private boolean hasRemainder = false; // set to true if there is data left over for the current row AND if we want
                                        // to keep processing it. Kill may be called by a limit in a subquery that
                                        // requires us to stop processing thecurrent row, but not stop processing
                                        // the data.
  private int remainderIndex = 0;
  private int recordCount;
  private MaterializedField unnestFieldMetadata;
  // Reference of TypedFieldId for Unnest column. It's always set in schemaChanged method and later used by others
  private TypedFieldId unnestTypedFieldId;
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

      // Get column size of unnest column.
      RecordBatchSizer.ColumnSize columnSize = getRecordBatchSizer().getColumn(unnestFieldMetadata.getName());

      final int rowIdColumnSize = TypeHelper.getSize(rowIdVector.getField().getType());

      // Average rowWidth of single element in the unnest list.
      // subtract the offset vector size from column data size.
      final int avgRowWidthSingleUnnestEntry = RecordBatchSizer
          .safeDivide(columnSize.getTotalNetSize() - (getOffsetVectorWidth() * columnSize.getValueCount()), columnSize
              .getElementCount());

      // Average rowWidth of outgoing batch.
      final int avgOutgoingRowWidth = avgRowWidthSingleUnnestEntry + rowIdColumnSize;

      // Number of rows in outgoing batch
      final int outputBatchSize = getOutputBatchSize();
      // Number of rows in outgoing batch
      setOutputRowCount(outputBatchSize, avgOutgoingRowWidth);

      setOutgoingRowWidth(avgOutgoingRowWidth);

      // Limit to lower bound of total number of rows possible for this batch
      // i.e. all rows fit within memory budget.
      setOutputRowCount(Math.min(columnSize.getElementCount(), getOutputRowCount()));
      RecordBatchStats.logRecordBatchStats(RecordBatchIOType.INPUT, getRecordBatchSizer(), getRecordBatchStatsContext());

      updateIncomingStats();
    }

  }


  public UnnestRecordBatch(UnnestPOP pop, FragmentContext context) throws OutOfMemoryException {
    super(pop, context);
    pop.addUnnestBatch(this);
    // get the output batch size from config.
    int configuredBatchSize = (int) context.getOptions().getOption(ExecConstants.OUTPUT_BATCH_SIZE_VALIDATOR);
    memoryManager = new UnnestMemoryManager(configuredBatchSize);
    rowIdColumnName = pop.getImplicitColumn();
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  protected void killIncoming(boolean sendUpstream) {
    //
    // In some cases we need to return a predetermined state from a call to next. These are:
    // 1) Kill is called due to an error occurring in the processing of the query. IterOutcome should be NONE
    // 2) Kill is called by LIMIT downstream from LATERAL. IterOutcome should be NONE
    // With PartitionLimitBatch occurring between Lateral and Unnest subquery, kill won't be triggered by it hence no
    // special handling is needed in that case.
    //
    Preconditions.checkNotNull(lateral);
    // Do not call kill on incoming. Lateral Join has the responsibility for killing incoming
    Preconditions.checkState(context.getExecutorState().isFailed() ||
      lateral.getLeftOutcome() == IterOutcome.STOP, "Kill received by unnest with unexpected state. " +
      "Neither the LateralOutcome is STOP nor executor state is failed");
      logger.debug("Kill received. Stopping all processing");
    state = BatchState.DONE;
    recordCount = 0;
    hasRemainder = false; // whatever the case, we need to stop processing the current row.
  }

  @Override
  public IterOutcome innerNext() {

    Preconditions.checkNotNull(lateral);

    // Short circuit if record batch has already sent all data and is done
    if (state == BatchState.DONE) {
      return IterOutcome.NONE;
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
        logger.debug("First batch received");
        schemaChanged(); // checks if schema has changed (redundant in this case becaause it has) AND saves the
                         // current field metadata for check in subsequent iterations
        setupNewSchema();
        stats.batchReceived(0, incoming.getRecordCount(), true);
        memoryManager.update();
        hasRemainder = incoming.getRecordCount() > 0;
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
      Preconditions.checkState(incoming.getRecordCount() > 0,
        "Incoming batch post buildSchema phase should never be empty for Unnest");
      container.zeroVectors();
      // Check if schema has changed
      if (lateral.getRecordIndex() == 0) {
        try {
          boolean hasNewSchema = schemaChanged();
          stats.batchReceived(0, incoming.getRecordCount(), hasNewSchema);
          if (hasNewSchema) {
            setupNewSchema();
            hasRemainder = true;
            memoryManager.update();
            return OK_NEW_SCHEMA;
          } else { // Unnest field schema didn't changed but new left empty/nonempty batch might come with OK_NEW_SCHEMA
            // This means even though there is no schema change for unnest field the reference of unnest field
            // ValueVector must have changed hence we should just refresh the transfer pairs and keep output vector
            // same as before. In case when new left batch is received with SchemaChange but was empty Lateral will
            // not call next on unnest and will change it's left outcome to OK. Whereas for non-empty batch next will
            // be called on unnest by Lateral. Hence UNNEST cannot rely on lateral current outcome to setup transfer
            // pair. It should do for each new left incoming batch.
            resetUnnestTransferPair();
            container.zeroVectors();
          } // else
          unnest.resetGroupIndex();
          memoryManager.update();
        } catch (SchemaChangeException ex) {
          kill(false);
          logger.error("Failure during query", ex);
          context.getExecutorState().fail(ex);
          return IterOutcome.STOP;
        }
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
    final MaterializedField field = incoming.getSchema().getColumn(unnestTypedFieldId.getFieldIds()[0]);
    final RepeatedValueVector vector;
    final ValueVector inVV =
        incoming.getValueAccessorById(field.getValueClass(), unnestTypedFieldId.getFieldIds()).getValueVector();

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
    unnest.setOutputCount(memoryManager.getOutputRowCount());
    final int incomingRecordCount = incoming.getRecordCount();

    int remainingRecordCount = unnest.getUnnestField().getAccessor().getInnerValueCount() - remainderIndex;

    // Allocate vector for rowId
    rowIdVector.allocateNew(Math.min(remainingRecordCount, memoryManager.getOutputRowCount()));

    //Expected output count is the num of values in the unnest column array
    final int childCount = incomingRecordCount == 0 ? 0 : remainingRecordCount;

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
    this.rowIdVector.getMutator().setValueCount(outputRecords);

    memoryManager.updateOutgoingStats(outputRecords);
    // If the current incoming record has spilled into two batches, we return
    // IterOutcome.OK so that the Lateral Join can keep calling next() until the
    // entire incoming recods has been unnested. If the entire records has been
    // unnested, we return EMIT and any blocking operators in the pipeline will
    // unblock.
    RecordBatchStats.logRecordBatchStats(RecordBatchIOType.OUTPUT, this, getRecordBatchStatsContext());
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
  @SuppressWarnings("resource")
  private TransferPair getUnnestFieldTransferPair(FieldReference reference) {
    final int[] typeFieldIds = unnestTypedFieldId.getFieldIds();
    final Class<?> vectorClass = incoming.getSchema().getColumn(typeFieldIds[0]).getValueClass();
    final ValueVector unnestField = incoming.getValueAccessorById(vectorClass, typeFieldIds).getValueVector();

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

  private TransferPair resetUnnestTransferPair() throws SchemaChangeException {
    final List<TransferPair> transfers = Lists.newArrayList();
    final FieldReference fieldReference = new FieldReference(popConfig.getColumn());
    final TransferPair transferPair = getUnnestFieldTransferPair(fieldReference);
    transfers.add(transferPair);
    logger.debug("Added transfer for unnest expression.");
    unnest.close();
    unnest.setup(context, incoming, this, transfers);
    setUnnestVector();
    return transferPair;
  }

  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    Preconditions.checkNotNull(lateral);
    container.clear();
    recordCount = 0;
    final MaterializedField rowIdField = MaterializedField.create(rowIdColumnName, Types.required(TypeProtos
        .MinorType.INT));
    this.rowIdVector= (IntVector)TypeHelper.getNewVector(rowIdField, oContext.getAllocator());
    container.add(rowIdVector);
    unnest = new UnnestImpl();
    unnest.setRowIdVector(rowIdVector);
    final TransferPair tp = resetUnnestTransferPair();
    container.add(TypeHelper.getNewVector(tp.getTo().getField(), oContext.getAllocator()));
    container.buildSchema(SelectionVectorMode.NONE);
    return true;
  }

  /**
   * Compares the schema of the unnest column in the current incoming with the schema of
   * the unnest column in the previous incoming.
   * Also saves the schema for comparison in future iterations
   *
   * @return true if the schema has changed, false otherwise
   */
  private boolean schemaChanged() throws SchemaChangeException {
    unnestTypedFieldId = checkAndGetUnnestFieldId();
    final MaterializedField thisField = incoming.getSchema().getColumn(unnestTypedFieldId.getFieldIds()[0]);
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

    RecordBatchStats.logRecordBatchStats(getRecordBatchStatsContext(),
      "incoming aggregate: batch count : %d, avg batch bytes : %d,  avg row bytes : %d, record count : %d",
      memoryManager.getNumIncomingBatches(), memoryManager.getAvgInputBatchSize(),
      memoryManager.getAvgInputRowWidth(), memoryManager.getTotalInputRecords());

    RecordBatchStats.logRecordBatchStats(getRecordBatchStatsContext(),
      "outgoing aggregate: batch count : %d, avg batch bytes : %d,  avg row bytes : %d, record count : %d",
      memoryManager.getNumOutgoingBatches(), memoryManager.getAvgOutputBatchSize(),
      memoryManager.getAvgOutputRowWidth(), memoryManager.getTotalOutputRecords());
  }

  private TypedFieldId checkAndGetUnnestFieldId() throws SchemaChangeException {
    final TypedFieldId fieldId = incoming.getValueVectorId(popConfig.getColumn());
    if (fieldId == null) {
      throw new SchemaChangeException(String.format("Unnest column %s not found inside the incoming record batch. " +
          "This may happen if a wrong Unnest column name is used in the query. Please rerun query after fixing that.",
        popConfig.getColumn()));
    }

    return fieldId;
  }

  @Override
  public void close() {
    updateStats();
    unnest.close();
    super.close();
  }

  @Override
  public void dump() {
    logger.error("UnnestRecordBatch[container={}, unnest={}, hasRemainder={}, remainderIndex={}, " +
            "unnestFieldMetadata={}]", container, unnest, hasRemainder, remainderIndex, unnestFieldMetadata);
  }
}
