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
package org.apache.drill.exec.physical.impl.join;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.LateralContract;
import org.apache.drill.exec.physical.config.LateralJoinPOP;
import org.apache.drill.exec.record.AbstractBinaryRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.JoinBatchMemoryManager;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.VectorAccessibleUtilities;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

import static org.apache.drill.exec.record.RecordBatch.IterOutcome.EMIT;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.NONE;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OK;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OK_NEW_SCHEMA;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OUT_OF_MEMORY;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.STOP;

/**
 * RecordBatch implementation for the lateral join operator. Currently it's expected LATERAL to co-exists with UNNEST
 * operator. Both LATERAL and UNNEST will share a contract with each other defined at {@link LateralContract}
 */
public class LateralJoinBatch extends AbstractBinaryRecordBatch<LateralJoinPOP> implements LateralContract {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LateralJoinBatch.class);

  // Maximum number records in the outgoing batch
  private int maxOutputRowCount;

  // Schema on the left side
  private BatchSchema leftSchema;

  // Schema on the right side
  private BatchSchema rightSchema;

  // Index in output batch to populate next row
  private int outputIndex;

  // Current index of record in left incoming which is being processed
  private int leftJoinIndex = -1;

  // Current index of record in right incoming which is being processed
  private int rightJoinIndex = -1;

  // flag to keep track if current left batch needs to be processed in future next call
  private boolean processLeftBatchInFuture;

  // Keep track if any matching right record was found for current left index record
  private boolean matchedRecordFound;

  // Used only for testing
  private boolean useMemoryManager = true;

  // Flag to keep track of new left batch so that update on memory manager is called only once per left batch
  private boolean isNewLeftBatch = false;

  /* ****************************************************************************************************************
   * Public Methods
   * ****************************************************************************************************************/
  public LateralJoinBatch(LateralJoinPOP popConfig, FragmentContext context,
                          RecordBatch left, RecordBatch right) throws OutOfMemoryException {
    super(popConfig, context, left, right);
    Preconditions.checkNotNull(left);
    Preconditions.checkNotNull(right);
    final int configOutputBatchSize = (int) context.getOptions().getOption(ExecConstants.OUTPUT_BATCH_SIZE_VALIDATOR);
    batchMemoryManager = new JoinBatchMemoryManager(configOutputBatchSize, left, right);

    // Initially it's set to default value of 64K and later for each new output row it will be set to the computed
    // row count
    maxOutputRowCount = batchMemoryManager.getOutputRowCount();
  }

  /**
   * Method that get's left and right incoming batch and produce the output batch. If the left incoming batch is
   * empty then next on right branch is not called and empty batch with correct outcome is returned. If non empty
   * left incoming batch is received then it call's next on right branch to get an incoming and finally produces
   * output.
   * @return IterOutcome state of the lateral join batch
   */
  @Override
  public IterOutcome innerNext() {

    // We don't do anything special on FIRST state. Process left batch first and then right batch if need be
    IterOutcome childOutcome = processLeftBatch();

    // reset this state after calling processLeftBatch above.
    processLeftBatchInFuture = false;

    // If the left batch doesn't have any record in the incoming batch (with OK_NEW_SCHEMA/EMIT) or the state returned
    // from left side is terminal state then just return the IterOutcome and don't call next() on right branch
    if (isTerminalOutcome(childOutcome) || left.getRecordCount() == 0) {
      container.setRecordCount(0);
      return childOutcome;
    }

    // Left side has some records in the batch so let's process right batch
    childOutcome = processRightBatch();

    // reset the left & right outcomes to OK here and send the empty batch downstream. Non-Empty right batch with
    // OK_NEW_SCHEMA will be handled in subsequent next call
    if (childOutcome == OK_NEW_SCHEMA) {
      leftUpstream = (leftUpstream != EMIT) ? OK : leftUpstream;
      rightUpstream = OK;
      return childOutcome;
    }

    if (isTerminalOutcome(childOutcome)) {
      return childOutcome;
    }

    // If OK_NEW_SCHEMA is seen only on non empty left batch but not on right batch, then we should setup schema in
    // output container based on new left schema and old right schema. If schema change failed then return STOP
    // downstream
    if (leftUpstream == OK_NEW_SCHEMA && !handleSchemaChange()) {
      return STOP;
    }

    // Setup the references of left, right and outgoing container in generated operator
    state = BatchState.NOT_FIRST;

    // Update the memory manager only if its a brand new incoming i.e. leftJoinIndex and rightJoinIndex is 0
    // Otherwise there will be a case where while filling last output batch, some records from previous left or
    // right batch are still left to be sent in output for which we will count this batch twice. The actual checks
    // are done in updateMemoryManager
    updateMemoryManager(LEFT_INDEX);

    // We have to call update on memory manager for empty batches (rightJoinIndex = -1) as well since other wise while
    // allocating memory for vectors below it can fail. Since in that case colSize will not have any info on right side
    // vectors and throws NPE. The actual checks are done in updateMemoryManager
    updateMemoryManager(RIGHT_INDEX);

    // allocate space for the outgoing batch
    allocateVectors();

    return produceOutputBatch();
  }

  @Override
  public void close() {
    updateBatchMemoryManagerStats();

    if (logger.isDebugEnabled()) {
      logger.debug("BATCH_STATS, incoming aggregate left: batch count : {}, avg bytes : {},  avg row bytes : {}, " +
        "record count : {}", batchMemoryManager.getNumIncomingBatches(JoinBatchMemoryManager.LEFT_INDEX),
        batchMemoryManager.getAvgInputBatchSize(JoinBatchMemoryManager.LEFT_INDEX),
        batchMemoryManager.getAvgInputRowWidth(JoinBatchMemoryManager.LEFT_INDEX),
        batchMemoryManager.getTotalInputRecords(JoinBatchMemoryManager.LEFT_INDEX));

      logger.debug("BATCH_STATS, incoming aggregate right: batch count : {}, avg bytes : {},  avg row bytes : {}, " +
        "record count : {}", batchMemoryManager.getNumIncomingBatches(JoinBatchMemoryManager.RIGHT_INDEX),
        batchMemoryManager.getAvgInputBatchSize(JoinBatchMemoryManager.RIGHT_INDEX),
        batchMemoryManager.getAvgInputRowWidth(JoinBatchMemoryManager.RIGHT_INDEX),
        batchMemoryManager.getTotalInputRecords(JoinBatchMemoryManager.RIGHT_INDEX));

      logger.debug("BATCH_STATS, outgoing aggregate: batch count : {}, avg bytes : {},  avg row bytes : {}, " +
        "record count : {}", batchMemoryManager.getNumOutgoingBatches(),
        batchMemoryManager.getAvgOutputBatchSize(),
        batchMemoryManager.getAvgOutputRowWidth(),
        batchMemoryManager.getTotalOutputRecords());
    }

    super.close();
  }

  @Override
  public int getRecordCount() {
    return container.getRecordCount();
  }

  /**
   * Returns the left side incoming for the Lateral Join. Used by right branch leaf operator of Lateral
   * to process the records at leftJoinIndex.
   *
   * @return - RecordBatch received as left side incoming
   */
  @Override
  public RecordBatch getIncoming() {
    Preconditions.checkState (left != null, "Retuning null left batch. It's unexpected since right side will only be " +
      "called iff there is any valid left batch");
    return left;
  }

  /**
   * Returns the current row index which the calling operator should process in current incoming left record batch.
   * LATERAL should never return it as -1 since that indicated current left batch is empty and LATERAL will never
   * call next on right side with empty left batch
   *
   * @return - int - index of row to process.
   */
  @Override
  public int getRecordIndex() {
    Preconditions.checkState (leftJoinIndex < left.getRecordCount(),
      String.format("Left join index: %d is out of bounds: %d", leftJoinIndex, left.getRecordCount()));
    return leftJoinIndex;
  }

  /**
   * Returns the current {@link org.apache.drill.exec.record.RecordBatch.IterOutcome} for the left incoming batch
   */
  @Override
  public IterOutcome getLeftOutcome() {
    return leftUpstream;
  }

  /* ****************************************************************************************************************
   * Protected Methods
   * ****************************************************************************************************************/

  /**
   * Method to get left and right batch during build schema phase for {@link LateralJoinBatch}. If left batch sees a
   * failure outcome then we don't even call next on right branch, since there is no left incoming.
   * @return true if both the left/right batch was received without failure outcome.
   *         false if either of batch is received with failure outcome.
   */
  @Override
  protected boolean prefetchFirstBatchFromBothSides() {
    // Left can get batch with zero or more records with OK_NEW_SCHEMA outcome as first batch
    leftUpstream = next(0, left);

    boolean validBatch = setBatchState(leftUpstream);

    if (validBatch) {
      isNewLeftBatch = true;
      rightUpstream = next(1, right);
      validBatch = setBatchState(rightUpstream);
    }

    // EMIT outcome is not expected as part of first batch from either side
    if (leftUpstream == EMIT || rightUpstream == EMIT) {
      state = BatchState.STOP;
      throw new IllegalStateException("Unexpected IterOutcome.EMIT received either from left or right side in " +
        "buildSchema phase");
    }
    return validBatch;
  }

  /**
   * Prefetch a batch from left and right branch to know about the schema of each side. Then adds value vector in
   * output container based on those schemas. For this phase LATERAL always expect's an empty batch from right side
   * which UNNEST should abide by.
   *
   * @throws SchemaChangeException if batch schema was changed during execution
   */
  @Override
  protected void buildSchema() throws SchemaChangeException {
    // Prefetch a RecordBatch from both left and right branch
    if (!prefetchFirstBatchFromBothSides()) {
      return;
    }
    Preconditions.checkState(right.getRecordCount() == 0, "Unexpected non-empty first right batch received");

    // Setup output container schema based on known left and right schema
    setupNewSchema();

    // Release the vectors received from right side
    VectorAccessibleUtilities.clear(right);

    // Set join index as invalid (-1) if the left side is empty, else set it to 0
    leftJoinIndex = (left.getRecordCount() <= 0) ? -1 : 0;
    rightJoinIndex = -1;

    // Reset the left side of the IterOutcome since for this call, OK_NEW_SCHEMA will be returned correctly
    // by buildSchema caller and we should treat the batch as received with OK outcome.
    leftUpstream = OK;
    rightUpstream = OK;
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    this.left.kill(sendUpstream);
    // Reset the left side outcome as STOP since as part of right kill when UNNEST will ask IterOutcome of left incoming
    // from LATERAL and based on that it can make decision if the kill is coming from downstream to LATERAL or upstream
    // to LATERAL. Like LIMIT operator being present downstream to LATERAL or upstream to LATERAL and downstream to
    // UNNEST.
    leftUpstream = STOP;
    this.right.kill(sendUpstream);
  }

  /* ****************************************************************************************************************
   * Private Methods
   * ****************************************************************************************************************/

  private boolean handleSchemaChange() {
    try {
      stats.startSetup();
      logger.debug(String.format("Setting up new schema based on incoming batch. Old output schema: %s",
        container.getSchema()));
      setupNewSchema();
      return true;
    } catch (SchemaChangeException ex) {
      logger.error("Failed to handle schema change hence killing the query");
      context.getExecutorState().fail(ex);
      left.kill(true); // Can have exchange receivers on left so called with true
      right.kill(false); // Won't have exchange receivers on right side
      return false;
    } finally {
      stats.stopSetup();
    }
  }

  private boolean isTerminalOutcome(IterOutcome outcome) {
    return (outcome == STOP || outcome == OUT_OF_MEMORY || outcome == NONE);
  }

  /**
   * Process left incoming batch with different {@link org.apache.drill.exec.record.RecordBatch.IterOutcome}. It is
   * called from main {@link LateralJoinBatch#innerNext()} block with each next() call from upstream operator. Also
   * when we populate the outgoing container then this method is called to get next left batch if current one is
   * fully processed. It calls next() on left side until we get a non-empty RecordBatch. OR we get either of
   * OK_NEW_SCHEMA/EMIT/NONE/STOP/OOM/NOT_YET outcome.
   * @return IterOutcome after processing current left batch
   */
  private IterOutcome processLeftBatch() {

    boolean needLeftBatch = leftJoinIndex == -1;

    // If left batch is empty
    while (needLeftBatch) {

      if (!processLeftBatchInFuture) {
        leftUpstream = next(LEFT_INDEX, left);
        isNewLeftBatch = true;
      }

      final boolean emptyLeftBatch = left.getRecordCount() <=0;
      logger.trace("Received a left batch and isEmpty: {}", emptyLeftBatch);

      switch (leftUpstream) {
        case OK_NEW_SCHEMA:
          // This OK_NEW_SCHEMA is received post build schema phase and from left side
          if (outputIndex > 0) { // can only reach here from produceOutputBatch
            // This means there is already some records from previous join inside left batch
            // So we need to pass that downstream and then handle the OK_NEW_SCHEMA in subsequent next call
            processLeftBatchInFuture = true;
            return OK_NEW_SCHEMA;
          }

          // If left batch is empty with actual schema change then just rebuild the output container and send empty
          // batch downstream
          if (emptyLeftBatch) {
            if (handleSchemaChange()) {
              leftJoinIndex = -1;
              return OK_NEW_SCHEMA;
            } else {
              return STOP;
            }
          } // else - setup the new schema information after getting it from right side too.
        case OK:
          // With OK outcome we will keep calling next until we get a batch with >0 records
          if (emptyLeftBatch) {
            leftJoinIndex = -1;
            continue;
          } else {
            leftJoinIndex = 0;
          }
          break;
        case EMIT:
          // don't call next on right batch
          if (emptyLeftBatch) {
            leftJoinIndex = -1;
            return EMIT;
          } else {
            leftJoinIndex = 0;
          }
          break;
        case OUT_OF_MEMORY:
        case NONE:
        case STOP:
          // Not using =0 since if outgoing container is empty then no point returning anything
          if (outputIndex > 0) { // can only reach here from produceOutputBatch
            processLeftBatchInFuture = true;
          }
          return leftUpstream;
        case NOT_YET:
          try {
            Thread.sleep(5);
          } catch (InterruptedException ex) {
            logger.debug("Thread interrupted while sleeping to call next on left branch of LATERAL since it " +
              "received NOT_YET");
          }
          break;
      }
      needLeftBatch = leftJoinIndex == -1;
    }
    return leftUpstream;
  }

  /**
   * Process right incoming batch with different {@link org.apache.drill.exec.record.RecordBatch.IterOutcome}. It is
   * called from main {@link LateralJoinBatch#innerNext()} block with each next() call from upstream operator and if
   * left batch has some records in it. Also when we populate the outgoing container then this method is called to
   * get next right batch if current one is fully processed.
   * @return IterOutcome after processing current left batch
   */
  private IterOutcome processRightBatch() {
    // Check if we still have records left to process in left incoming from new batch or previously half processed
    // batch based on indexes. We are making sure to update leftJoinIndex and rightJoinIndex correctly. Like for new
    // batch leftJoinIndex will always be set to zero and once leftSide batch is fully processed then it will be set
    // to -1.
    // Whereas rightJoinIndex is to keep track of record in right batch being joined with record in left batch.
    // So when there are cases such that all records in right batch is not consumed by the output, then rightJoinIndex
    // will be a valid index. When all records are consumed it will be set to -1.
    boolean needNewRightBatch = (leftJoinIndex >= 0) && (rightJoinIndex == -1);
    while (needNewRightBatch) {
      rightUpstream = next(RIGHT_INDEX, right);
      switch (rightUpstream) {
        case OK_NEW_SCHEMA:

          // If there is some records in the output batch that means left batch didn't came with OK_NEW_SCHEMA,
          // otherwise it would have been marked for processInFuture and output will be returned. This means for
          // current non processed left or new left non-empty batch there is unexpected right batch schema change
          if (outputIndex > 0) {
            throw new IllegalStateException("SchemaChange on right batch is not expected in between the rows of " +
              "current left batch or a new non-empty left batch with no schema change");
          }
          // We should not get OK_NEW_SCHEMA multiple times for the same left incoming batch. So there won't be a
          // case where we get OK_NEW_SCHEMA --> OK (with batch) ---> OK_NEW_SCHEMA --> OK/EMIT fall through
          //
          // Right batch with OK_NEW_SCHEMA can be non-empty so update the rightJoinIndex correctly and pass the
          // new schema downstream with empty batch and later with subsequent next() call the join output will be
          // produced
          if (handleSchemaChange()) {
            container.setRecordCount(0);
            rightJoinIndex = (right.getRecordCount() > 0) ? 0 : -1;
            return OK_NEW_SCHEMA;
          } else {
            return STOP;
          }
        case OK:
        case EMIT:
          // Even if there are no records we should not call next() again because in case of LEFT join empty batch is
          // of importance too
          rightJoinIndex = (right.getRecordCount() > 0) ? 0 : -1;
          needNewRightBatch = false;
          break;
        case OUT_OF_MEMORY:
        case NONE:
        case STOP:
          needNewRightBatch = false;
          break;
        case NOT_YET:
          try {
            Thread.sleep(10);
          } catch (InterruptedException ex) {
            logger.debug("Thread interrupted while sleeping to call next on left branch of LATERAL since it " +
              "received NOT_YET");
          }
          break;
      }
    }
    return rightUpstream;
  }

  /**
   * Get's the current left and right incoming batch and does the cross join to fill the output batch. If all the
   * records in the either or both the batches are consumed then it get's next batch from that branch depending upon
   * if output batch still has some space left. If output batch is full then the output if finalized to be sent
   * downstream. Subsequent call's knows how to consume previously half consumed (if any) batches and producing the
   * output using that.
   *
   * @return - IterOutcome to be send along with output batch to downstream operator
   */
  private IterOutcome produceOutputBatch() {

    boolean isLeftProcessed = false;

    // Try to fully pack the outgoing container
    while (!isOutgoingBatchFull()) {
      final int previousOutputCount = outputIndex;
      // invoke the runtime generated method to emit records in the output batch for each leftJoinIndex
      crossJoinAndOutputRecords();

      // We have produced some records in outgoing container, hence there must be a match found for left record
      if (outputIndex > previousOutputCount) {
        // Need this extra flag since there can be left join case where for current leftJoinIndex it receives a right
        // batch with data, then an empty batch and again another empty batch with EMIT outcome. If we just use
        // outputIndex then we will loose the information that few rows for leftJoinIndex is already produced using
        // first right batch
        matchedRecordFound = true;
      }

      // One right batch might span across multiple output batch. So rightIndex will be moving sum of all the
      // output records for this record batch until it's fully consumed.
      //
      // Also it can be so that one output batch can contain records from 2 different right batch hence the
      // rightJoinIndex should move by number of records in output batch for current right batch only.
      rightJoinIndex += outputIndex - previousOutputCount;
      final boolean isRightProcessed = rightJoinIndex == -1 || rightJoinIndex >= right.getRecordCount();

      // Check if above join to produce output was based on empty right batch or
      // it resulted in right side batch to be fully consumed. In this scenario only if rightUpstream
      // is EMIT then increase the leftJoinIndex.
      // Otherwise it means for the given right batch there is still some record left to be processed.
      if (isRightProcessed) {
        if (rightUpstream == EMIT) {
          if (!matchedRecordFound && JoinRelType.LEFT == popConfig.getJoinType()) {
            // copy left side in case of LEFT join
            emitLeft(leftJoinIndex, outputIndex, 1);
            ++outputIndex;
          }
          ++leftJoinIndex;
          // Reset matchedRecord for next left index record
          matchedRecordFound = false;
        }

        // Release vectors of right batch. This will happen for both rightUpstream = EMIT/OK
        VectorAccessibleUtilities.clear(right);
        rightJoinIndex = -1;
      }

      // Check if previous left record was last one, then set leftJoinIndex to -1
      isLeftProcessed = leftJoinIndex >= left.getRecordCount();
      if (isLeftProcessed) {
        leftJoinIndex = -1;
        VectorAccessibleUtilities.clear(left);
      }

      // Check if output batch still has some space
      if (!isOutgoingBatchFull()) {
        // Check if left side still has records or not
        if (isLeftProcessed) {
          // The current left batch was with EMIT/OK_NEW_SCHEMA outcome, then return output to downstream layer before
          // getting next batch
          if (leftUpstream == EMIT || leftUpstream == OK_NEW_SCHEMA) {
            break;
          } else {
            logger.debug("Output batch still has some space left, getting new batches from left and right");
            // Get both left batch and the right batch and make sure indexes are properly set
            leftUpstream = processLeftBatch();

            // output batch is not empty and we have new left batch with OK_NEW_SCHEMA or terminal outcome
            if (processLeftBatchInFuture) {
              logger.debug("Received left batch with outcome {} such that we have to return the current outgoing " +
                "batch and process the new batch in subsequent next call", leftUpstream);
              // We should return the current output batch with OK outcome and don't reset the leftUpstream
              finalizeOutputContainer();
              return OK;
            }

            // If left batch received a terminal outcome then don't call right batch
            if (isTerminalOutcome(leftUpstream)) {
              finalizeOutputContainer();
              return leftUpstream;
            }

            // If we have received the left batch with EMIT outcome and is empty then we should return previous output
            // batch with EMIT outcome
            if ((leftUpstream == EMIT || leftUpstream == OK_NEW_SCHEMA) && left.getRecordCount() == 0) {
              isLeftProcessed = true;
              break;
            }

            // Update the batch memory manager to use new left incoming batch
            updateMemoryManager(LEFT_INDEX);
          }
        }

        // If we are here it means one of the below:
        // 1) Either previous left batch was not fully processed and it came with OK outcome. There is still some space
        // left in outgoing batch so let's get next right batch.
        // 2) OR previous left & right batch was fully processed and it came with OK outcome. There is space in outgoing
        // batch. Now we have got new left batch with OK outcome. Let's get next right batch
        // 3) OR previous left & right batch was fully processed and left came with OK outcome. Outgoing batch is
        // empty since all right batches were empty for all left rows. Now we got another non-empty left batch with
        // OK_NEW_SCHEMA.
        rightUpstream = processRightBatch();
        if (rightUpstream == OK_NEW_SCHEMA) {
          leftUpstream = (leftUpstream != EMIT) ? OK : leftUpstream;
          rightUpstream = OK;
          finalizeOutputContainer();
          return OK_NEW_SCHEMA;
        }

        if (isTerminalOutcome(rightUpstream)) {
          finalizeOutputContainer();
          return rightUpstream;
        }

        // Update the batch memory manager to use new right incoming batch
        updateMemoryManager(RIGHT_INDEX);

        // If OK_NEW_SCHEMA is seen only on non empty left batch but not on right batch, then we should setup schema in
        // output container based on new left schema and old right schema. If schema change failed then return STOP
        // downstream
        if (leftUpstream == OK_NEW_SCHEMA && isLeftProcessed) {
          if (!handleSchemaChange()) {
            return STOP;
          }
          // Since schema has change so we have new empty vectors in output container hence allocateMemory for them
          allocateVectors();
        }
      }
    } // output batch is full to its max capacity

    finalizeOutputContainer();

    // Check if output batch was full and left was fully consumed or not. Since if left is not consumed entirely
    // but output batch is full, then if the left batch came with EMIT outcome we should send this output batch along
    // with OK outcome not with EMIT. Whereas if output is full and left is also fully consumed then we should send
    // EMIT outcome.
    if (leftUpstream == EMIT && isLeftProcessed) {
      logger.debug("Sending current output batch with EMIT outcome since left is received with EMIT and is fully " +
        "consumed in output batch");
      return EMIT;
    }

    if (leftUpstream == OK_NEW_SCHEMA) {
      // return output batch with OK_NEW_SCHEMA and reset the state to OK
      logger.debug("Sending current output batch with OK_NEW_SCHEMA and resetting the left outcome to OK for next set" +
        " of batches");
      leftUpstream = OK;
      return OK_NEW_SCHEMA;
    }
    return OK;
  }

  /**
   * Finalizes the current output container with the records produced so far before sending it downstream
   */
  private void finalizeOutputContainer() {
    VectorAccessibleUtilities.setValueCount(container, outputIndex);

    // Set the record count in the container
    container.setRecordCount(outputIndex);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    batchMemoryManager.updateOutgoingStats(outputIndex);

    if (logger.isDebugEnabled()) {
      logger.debug("BATCH_STATS, outgoing:\n {}", new RecordBatchSizer(this));
      logger.debug("Number of records emitted: {} and Allocator Stats: [AllocatedMem: {}, PeakMem: {}]",
        outputIndex, container.getAllocator().getAllocatedMemory(), container.getAllocator().getPeakMemoryAllocation());
    }

    // Update the output index for next output batch to zero
    outputIndex = 0;
  }

  /**
   * Check if the schema changed between provided newSchema and oldSchema. It relies on
   * {@link BatchSchema#isEquivalent(BatchSchema)}.
   * @param newSchema - New Schema information
   * @param oldSchema -  - New Schema information to compare with
   *
   * @return - true - if newSchema is not same as oldSchema
   *         - false - if newSchema is same as oldSchema
   */
  private boolean isSchemaChanged(BatchSchema newSchema, BatchSchema oldSchema) {
    return (newSchema == null || oldSchema == null) || !newSchema.isEquivalent(oldSchema);
  }

  /**
   * Validate if the input schema is not null and doesn't contain any Selection Vector.
   * @param schema - input schema to verify
   * @return - true: valid input schema
   *           false: invalid input schema
   */
  private boolean verifyInputSchema(BatchSchema schema) {

    boolean isValid = true;
    if (schema == null) {
      logger.error("Null schema found for the incoming batch");
      isValid = false;
    } else {
      final BatchSchema.SelectionVectorMode svMode = schema.getSelectionVectorMode();
      if (svMode != BatchSchema.SelectionVectorMode.NONE) {
        logger.error("Incoming batch schema found with selection vector which is not supported. SVMode: {}",
          svMode.toString());
        isValid = false;
      }
    }
    return isValid;
  }

  /**
   * Helps to create the outgoing container vectors based on known left and right batch schemas
   * @throws SchemaChangeException
   */
  private void setupNewSchema() throws SchemaChangeException {

    logger.debug(String.format("Setting up new schema based on incoming batch. New left schema: %s" +
        " and New right schema: %s", left.getSchema(), right.getSchema()));

    // Clear up the container
    container.clear();
    leftSchema = left.getSchema();
    rightSchema = right.getSchema();

    if (!verifyInputSchema(leftSchema)) {
      throw new SchemaChangeException("Invalid Schema found for left incoming batch");
    }

    if (!verifyInputSchema(rightSchema)) {
      throw new SchemaChangeException("Invalid Schema found for right incoming batch");
    }

    // Setup LeftSchema in outgoing container
    for (final VectorWrapper<?> vectorWrapper : left) {
      container.addOrGet(vectorWrapper.getField());
    }

    // Setup RightSchema in the outgoing container
    for (final VectorWrapper<?> vectorWrapper : right) {
      MaterializedField rightField = vectorWrapper.getField();
      TypeProtos.MajorType rightFieldType = vectorWrapper.getField().getType();

      // make right input schema optional if we have LEFT join
      if (popConfig.getJoinType() == JoinRelType.LEFT &&
        rightFieldType.getMode() == TypeProtos.DataMode.REQUIRED) {
        final TypeProtos.MajorType outputType =
          Types.overrideMode(rightField.getType(), TypeProtos.DataMode.OPTIONAL);

        // Create the right field with optional type. This will also take care of creating
        // children fields in case of ValueVectors of map type
        rightField = rightField.withType(outputType);
      }
      container.addOrGet(rightField);
    }

    // Let's build schema for the container
    outputIndex = 0;
    container.setRecordCount(outputIndex);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    logger.debug("Output Schema created {} based on input left schema {} and right schema {}", container.getSchema(),
      leftSchema, rightSchema);
  }

  /**
   * Simple method to allocate space for all the vectors in the container.
   */
  private void allocateVectors() {
    for (VectorWrapper w : container) {
      RecordBatchSizer.ColumnSize colSize = batchMemoryManager.getColumnSize(w.getField().getName());
      colSize.allocateVector(w.getValueVector(), maxOutputRowCount);
    }

    logger.debug("Allocator Stats: [AllocatedMem: {}, PeakMem: {}]", container.getAllocator().getAllocatedMemory(),
      container.getAllocator().getPeakMemoryAllocation());
  }

  private boolean setBatchState(IterOutcome outcome) {
    switch(outcome) {
      case STOP:
      case EMIT:
        state = BatchState.STOP;
        return false;
      case OUT_OF_MEMORY:
        state = BatchState.OUT_OF_MEMORY;
        return false;
      case NONE:
      case NOT_YET:
        state = BatchState.DONE;
        return false;
    }
    return true;
  }

  /**
   * Main entry point for producing the output records. This method populates the output batch after cross join of
   * the record in a given left batch at left index and all the corresponding right batches produced for
   * this left index. The right container is copied starting from rightIndex until number of records in the container.
   */
  private void crossJoinAndOutputRecords() {
    final int rightRecordCount = right.getRecordCount();

    // If there is no record in right batch just return current index in output batch
    if (rightRecordCount <= 0) {
      return;
    }

    // Check if right batch is empty since we have to handle left join case
    Preconditions.checkState(rightJoinIndex != -1, "Right batch record count is >0 but index is -1");

    int currentOutIndex = outputIndex;
    // Number of rows that can be copied in output batch
    final int maxAvailableRowSlot = maxOutputRowCount - currentOutIndex;
    // Number of rows that can be copied inside output batch is minimum of available slot in
    // output batch and available data to copy from right side. It can be half consumed right batch
    // which has few more rows to be copied to output but output batch has more to fill.
    final int rowsToCopy = Math.min(maxAvailableRowSlot, (rightRecordCount - rightJoinIndex));

    if (logger.isDebugEnabled()) {
      logger.debug("Producing output for leftIndex: {}, rightIndex: {}, rightRecordCount: {}, outputIndex: {} and " +
        "availableSlotInOutput: {}", leftJoinIndex, rightJoinIndex, rightRecordCount, outputIndex, maxAvailableRowSlot);
      logger.debug("Output Batch stats before copying new data: {}", new RecordBatchSizer(this));
    }

    // First copy all the left vectors data. Doing it in this way since it's the same data being copied over may be
    // we will have performance gain from JVM
    emitLeft(leftJoinIndex, currentOutIndex, rowsToCopy);

    // Copy all the right side vectors data
    emitRight(rightJoinIndex, currentOutIndex, rowsToCopy);

    // Update outputIndex
    outputIndex += rowsToCopy;
  }

  /**
   * Given a record batch, copies data from all it's vectors at fromRowIndex to all the vectors in output batch at
   * toRowIndex. It iterates over all the vectors from startVectorIndex to endVectorIndex inside the record batch to
   * copy the data and copies it inside vectors from startVectorIndex + baseVectorIndex to endVectorIndex +
   * baseVectorIndex.
   * @param fromRowIndex - row index of all the vectors in batch to copy data from
   * @param toRowIndex - row index of all the vectors in outgoing batch to copy data to
   * @param batch - source record batch holding vectors with data
   * @param startVectorIndex - start index of vector inside source record batch
   * @param endVectorIndex - end index of vector inside source record batch
   * @param baseVectorIndex - base index to be added to startVectorIndex to get corresponding vector in outgoing batch
   * @param numRowsToCopy - Number of rows to copy into output batch
   * @param moveFromIndex - boolean to indicate if the fromIndex should also be increased or not. Since in case of
   *                      copying data from left vector fromIndex is constant whereas in case of copying data from right
   *                      vector fromIndex will move along with output index.
   */
  private void copyDataToOutputVectors(int fromRowIndex, int toRowIndex, RecordBatch batch,
                                       int startVectorIndex, int endVectorIndex, int baseVectorIndex,
                                       int numRowsToCopy, boolean moveFromIndex) {
    // Get the vectors using field index rather than Materialized field since input batch field can be different from
    // output container field in case of Left Join. As we rebuild the right Schema field to be optional for output
    // container.
    for (int i = startVectorIndex; i < endVectorIndex; ++i) {
      // Get input vector
      final Class<?> inputValueClass = batch.getSchema().getColumn(i).getValueClass();
      final ValueVector inputVector = batch.getValueAccessorById(inputValueClass, i).getValueVector();

      // Get output vector
      final int outputVectorIndex = i + baseVectorIndex;
      final Class<?> outputValueClass = this.getSchema().getColumn(outputVectorIndex).getValueClass();
      final ValueVector outputVector = this.getValueAccessorById(outputValueClass, outputVectorIndex).getValueVector();

      logger.trace("Copying data from incoming batch vector to outgoing batch vector. [IncomingBatch: " +
          "(RowIndex: {}, VectorType: {}), OutputBatch: (RowIndex: {}, VectorType: {}) and Other: (TimeEachValue: {}," +
          " NumBaseIndex: {}) ]",
        fromRowIndex, inputValueClass, toRowIndex, outputValueClass, numRowsToCopy, baseVectorIndex);

      // Copy data from input vector to output vector for numRowsToCopy times.
      for (int j = 0; j < numRowsToCopy; ++j) {
        outputVector.copyEntry(toRowIndex + j, inputVector, (moveFromIndex) ? fromRowIndex + j : fromRowIndex);
      }
    }
  }

  /**
   * Copies data at leftIndex from each of vector's in left incoming batch to outIndex at corresponding vectors in
   * outgoing record batch
   * @param leftIndex - index to copy data from left incoming batch vectors
   * @param outIndex - index to copy data to in outgoing batch vectors
   */
  private void emitLeft(int leftIndex, int outIndex, int numRowsToCopy) {
    copyDataToOutputVectors(leftIndex, outIndex, left, 0,
      leftSchema.getFieldCount(), 0, numRowsToCopy, false);
  }

  /**
   * Copies data at rightIndex from each of vector's in right incoming batch to outIndex at corresponding vectors in
   * outgoing record batch
   * @param rightIndex - index to copy data from right incoming batch vectors
   * @param outIndex - index to copy data to in outgoing batch vectors
   */
  private void emitRight(int rightIndex, int outIndex, int numRowsToCopy) {
    copyDataToOutputVectors(rightIndex, outIndex, right, 0,
      rightSchema.getFieldCount(), leftSchema.getFieldCount(), numRowsToCopy, true);
  }

  /**
   * Used only for testing for cases when multiple output batches are produced for same input set
   * @param outputRowCount - Max rows that output batch can hold
   */
  @VisibleForTesting
  public void setMaxOutputRowCount(int outputRowCount) {
    maxOutputRowCount = outputRowCount;
  }

  /**
   * Used only for testing to disable output batch calculation using memory manager and instead use the static max
   * value set by {@link LateralJoinBatch#setMaxOutputRowCount(int)}
   * @param useMemoryManager - false - disable memory manager update to take effect, true enable memory manager update
   */
  @VisibleForTesting
  public void setUseMemoryManager(boolean useMemoryManager) {
    this.useMemoryManager = useMemoryManager;
  }

  private boolean isOutgoingBatchFull() {
    return outputIndex >= maxOutputRowCount;
  }

  private void updateMemoryManager(int inputIndex) {

    if (inputIndex == LEFT_INDEX && isNewLeftBatch) {
      // reset state and continue to update
      isNewLeftBatch = false;
    } else if (inputIndex == RIGHT_INDEX && (rightJoinIndex == 0 || rightJoinIndex == -1)) {
      // continue to update
    } else {
      return;
    }

    // For cases where all the previous input were consumed and send with previous output batch. But now we are building
    // a new output batch with new incoming then it will not cause any problem since outputIndex will be 0
    final int newOutputRowCount = batchMemoryManager.update(inputIndex, outputIndex);

    if (logger.isDebugEnabled()) {
      logger.debug("BATCH_STATS, incoming {}:\n {}", inputIndex == LEFT_INDEX ? "left" : "right",
        batchMemoryManager.getRecordBatchSizer(inputIndex));
      logger.debug("Previous OutputRowCount: {}, New OutputRowCount: {}", maxOutputRowCount, newOutputRowCount);
    }

    if (useMemoryManager) {
      maxOutputRowCount = newOutputRowCount;
    }
  }
}
