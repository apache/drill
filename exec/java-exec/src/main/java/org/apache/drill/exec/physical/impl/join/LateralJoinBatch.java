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

import com.google.common.base.Preconditions;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.LateralContract;
import org.apache.drill.exec.physical.config.LateralJoinPOP;
import org.apache.drill.exec.record.AbstractBinaryRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessibleUtilities;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.AllocationHelper;

import java.io.IOException;

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
  // Made public for testing
  static int MAX_BATCH_SIZE = 4096;

  // Input indexes to correctly update the stats
  private static final int LEFT_INPUT = 0;

  private static final int RIGHT_INPUT = 1;

  // Schema on the left side
  private BatchSchema leftSchema = null;

  // Schema on the right side
  private BatchSchema rightSchema = null;

  // Runtime generated class implementing the LateralJoin interface
  private LateralJoin lateralJoiner = null;

  // Number of output records in the current outgoing batch
  private int outputRecords = 0;

  // Current index of record in left incoming which is being processed
  private int leftJoinIndex = -1;

  // Current index of record in right incoming which is being processed
  private int rightJoinIndex = -1;

  // flag to keep track if current left batch needs to be processed in future next call
  private boolean processLeftBatchInFuture = false;

  // Keep track if any matching right record was found for current left index record
  private boolean matchedRecordFound = false;

  private boolean enableLateralCGDebugging = true;

  // Shared Generator mapping for the left/right side : constant
  private static final GeneratorMapping EMIT_CONSTANT =
    GeneratorMapping.create("doSetup"/* setup method */,"doSetup" /* eval method */,
      null /* reset */, null /* cleanup */);

  // Generator mapping for the right side
  private static final GeneratorMapping EMIT_RIGHT =
    GeneratorMapping.create("doSetup"/* setup method */,"emitRight" /* eval method */,
      null /* reset */,null /* cleanup */);

  // Generator mapping for the left side : scalar
  private static final GeneratorMapping EMIT_LEFT =
    GeneratorMapping.create("doSetup" /* setup method */, "emitLeft" /* eval method */,
      null /* reset */, null /* cleanup */);

  // Mapping set for the right side
  private static final MappingSet emitRightMapping =
    new MappingSet("rightIndex" /* read index */, "outIndex" /* write index */,
      "rightBatch" /* read container */,"outgoing" /* write container */,
      EMIT_CONSTANT, EMIT_RIGHT);

  // Mapping set for the left side
  private static final MappingSet emitLeftMapping =
    new MappingSet("leftIndex" /* read index */, "outIndex" /* write index */,
      "leftBatch" /* read container */,"outgoing" /* write container */,
      EMIT_CONSTANT, EMIT_LEFT);

  protected LateralJoinBatch(LateralJoinPOP popConfig, FragmentContext context,
                             RecordBatch left, RecordBatch right) throws OutOfMemoryException {
    super(popConfig, context, left, right);
    Preconditions.checkNotNull(left);
    Preconditions.checkNotNull(right);
    enableLateralCGDebugging = true;//context.getConfig().getBoolean(ExecConstants.ENABLE_CODE_DUMP_DEBUG_LATERAL);
  }

  private boolean handleSchemaChange() {
    try {
      stats.startSetup();
      setupNewSchema();
      lateralJoiner = setupWorker();
      lateralJoiner.setupLateralJoin(context, left, right, this, popConfig.getJoinType());
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
      leftUpstream = !processLeftBatchInFuture ? next(LEFT_INPUT, left) : leftUpstream;
      final boolean emptyLeftBatch = left.getRecordCount() <=0;

      switch (leftUpstream) {
        case OK_NEW_SCHEMA:
          // This OK_NEW_SCHEMA is received post build schema phase and from left side
          // If schema didn't actually changed then just handle it as OK outcome
          if (!isSchemaChanged(left.getSchema(), leftSchema)) {
            logger.warn("New schema received from left side is same as previous known left schema. Ignoring this " +
              "schema change");

            // Current left batch is empty and schema didn't changed as well, so let's get next batch and loose
            // OK_NEW_SCHEMA outcome
            processLeftBatchInFuture = false;
            if (emptyLeftBatch) {
              continue;
            } else {
              leftUpstream = OK;
            }
          } else if (outputRecords > 0) {
            // This means there is already some records from previous join inside left batch
            // So we need to pass that downstream and then handle the OK_NEW_SCHEMA in subsequent next call
            processLeftBatchInFuture = true;
            return OK_NEW_SCHEMA;
          }

          // If left batch is empty with actual schema change then just rebuild the output container and send empty
          // batch downstream
          if (emptyLeftBatch) {
            if (handleSchemaChange()) {
              //container.setRecordCount(0);
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
            //container.setRecordCount(0);
            return EMIT;
          } else {
            leftJoinIndex = 0;
          }
          break;
        case OUT_OF_MEMORY:
        case NONE:
        case STOP:
          // Not using =0 since if outgoing container is empty then no point returning anything
          if (outputRecords > 0) {
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
    // batch leftJoinIndex will always be set to zero and once leftSide batch is fully processed then
    // it will be set to -1.
    // Whereas rightJoinIndex is to keep track of record in right batch being joined with
    // record in left batch. So when there are cases such that all records in right batch is not consumed
    // by the output, then rightJoinIndex will be a valid index. When all records are consumed it will be set to -1.
    boolean needNewRightBatch = (leftJoinIndex >= 0) && (rightJoinIndex == -1);
    while (needNewRightBatch) {
      rightUpstream = next(RIGHT_INPUT, right);
      switch (rightUpstream) {
        case OK_NEW_SCHEMA:
          // We should not get OK_NEW_SCHEMA multiple times for the same left incoming batch. So there won't be a
          // case where we get OK_NEW_SCHEMA --> OK (with batch) ---> OK_NEW_SCHEMA --> OK/EMIT
          // fall through
          //
          // Right batch with OK_NEW_SCHEMA is always going to be an empty batch, so let's pass the new schema
          // downstream and later with subsequent next() call the join output will be produced
          Preconditions.checkState(right.getRecordCount() == 0,
            "Right side batch with OK_NEW_SCHEMA is not empty");

          if (!isSchemaChanged(right.getSchema(), rightSchema)) {
            logger.warn("New schema received from right side is same as previous known right schema. Ignoring this " + "schema change");
            continue;
          }
          if (handleSchemaChange()) {
            container.setRecordCount(0);
            rightJoinIndex = -1;
            return OK_NEW_SCHEMA;
          } else {
            return STOP;
          }
        case OK:
        case EMIT:
          // Even if there are no records we should not call next() again because in case of LEFT join
          // empty batch is of importance too
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

    // If the left batch doesn't have any record in the incoming batch or the state returned from
    // left side is terminal state then just return the IterOutcome and don't call next() on
    // right branch
    if (left.getRecordCount() == 0 || isTerminalOutcome(childOutcome)) {
      container.setRecordCount(0);
      return childOutcome;
    }

    // Left side has some records in the batch so let's process right batch
    childOutcome = processRightBatch();

    // reset the left & right outcomes to OK here and send the empty batch downstream
    // Assumption being right side will always send OK_NEW_SCHEMA with empty batch which is what UNNEST will do
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
    if (state == BatchState.FIRST) {
      lateralJoiner.setupLateralJoin(context, left, right, this, popConfig.getJoinType());
      state = BatchState.NOT_FIRST;
    }

    // allocate space for the outgoing batch
    allocateVectors();
    return produceOutputBatch();
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
    while (outputRecords < LateralJoinBatch.MAX_BATCH_SIZE) {
      int previousOutputCount = outputRecords;
      // invoke the runtime generated method to emit records in the output batch for each leftJoinIndex
      outputRecords = lateralJoiner.crossJoinAndOutputRecords(leftJoinIndex, rightJoinIndex);

      // We have produced some records in outgoing container, hence there must be a match found for left record
      if (outputRecords > previousOutputCount) {
        matchedRecordFound = true;
      }

      if (right.getRecordCount() == 0) {
        rightJoinIndex = -1;
      } else {
        // One right batch might span across multiple output batch. So rightIndex will be moving sum of all the
        // output records for this record batch until it's fully consumed.
        //
        // Also it can be so that one output batch can contain records from 2 different right batch hence the
        // rightJoinIndex should move by number of records in output batch for current right batch only.
        rightJoinIndex += outputRecords - previousOutputCount;
      }

      final boolean isRightProcessed = rightJoinIndex == -1 || rightJoinIndex >= right.getRecordCount();

      // Check if above join to produce output was based on empty right batch or
      // it resulted in right side batch to be fully consumed. In this scenario only if rightUpstream
      // is EMIT then increase the leftJoinIndex.
      // Otherwise it means for the given right batch there is still some record left to be processed.
      if (isRightProcessed) {
        if (rightUpstream == EMIT) {
          if (!matchedRecordFound) {
            // will only produce left side in case of LEFT join
            outputRecords = lateralJoiner.generateLeftJoinOutput(leftJoinIndex);
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
      if (outputRecords < MAX_BATCH_SIZE) {
        // Check if left side still has records or not
        if (isLeftProcessed) {
          // The left batch was with EMIT/OK_NEW_SCHEMA outcome, then return output to downstream layer
          if (leftUpstream == EMIT || leftUpstream == OK_NEW_SCHEMA) {
            break;
          } else {
            // Get both left batch and the right batch and make sure indexes are properly set
            leftUpstream = processLeftBatch();

            if (processLeftBatchInFuture) {
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
            if (leftUpstream == EMIT && left.getRecordCount() == 0) {
              break;
            }
          }
        }

        // If we are here it means one of the below:
        // 1) Either previous left batch was not fully processed and it came with OK outcome. There is still some space
        // left in outgoing batch so let's get next right batch.
        // 2) OR previous left & right batch was fully processed and it came with OK outcome. There is space in outgoing
        // batch. Now we have got new left batch with OK outcome. Let's get next right batch
        //
        // It will not hit OK_NEW_SCHEMA since left side have not seen that outcome

        rightUpstream = processRightBatch();

        Preconditions.checkState(rightUpstream != OK_NEW_SCHEMA, "Unexpected schema change in right branch");

        if (isTerminalOutcome(rightUpstream)) {
          finalizeOutputContainer();
          return rightUpstream;
        }
      }
    } // output batch is full to its max capacity

    finalizeOutputContainer();

    // Check if output batch was full and left was fully consumed or not. Since if left is not consumed entirely
    // but output batch is full, then if the left batch came with EMIT outcome we should send this output batch along
    // with OK outcome not with EMIT. Whereas if output is full and left is also fully consumed then we should send
    // EMIT outcome.
    if (leftUpstream == EMIT && isLeftProcessed) {
      return EMIT;
    }

    if (leftUpstream == OK_NEW_SCHEMA) {
      // return output batch with OK_NEW_SCHEMA and reset the state to OK
      leftUpstream = OK;
      return OK_NEW_SCHEMA;
    }
    return OK;
  }

  /**
   * Finalizes the current output container with the records produced so far before sending it downstream
   */
  private void finalizeOutputContainer() {

    VectorAccessibleUtilities.setValueCount(container, outputRecords);

    // Set the record count in the container
    container.setRecordCount(outputRecords);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    logger.debug("Number of records emitted: " + outputRecords);

    // We are about to send the output batch so reset the outputRecords for future next call
    outputRecords = 0;
    // Update the output index for next output batch to zero
    lateralJoiner.updateOutputIndex(0);
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
    container.setRecordCount(0);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
  }

  /**
   * Method generates the runtime code needed for LateralJoin. Other than the setup method to set the input and output
   * value vector references we implement two more methods
   * 1. emitLeft() -> Copy record from the left side to output container
   * 2. emitRight() -> Copy record from the right side to output container
   * @return the runtime generated class that implements the LateralJoin interface
   */
  private LateralJoin setupWorker() throws SchemaChangeException {
    final CodeGenerator<LateralJoin> lateralCG = CodeGenerator.get(
      LateralJoin.TEMPLATE_DEFINITION, context.getOptions());
    lateralCG.plainJavaCapable(true);

    // To enabled code gen dump for lateral use the setting ExecConstants.ENABLE_CODE_DUMP_DEBUG_LATERAL
    lateralCG.saveCodeForDebugging(enableLateralCGDebugging);
    final ClassGenerator<LateralJoin> nLJClassGenerator = lateralCG.getRoot();

    // generate emitLeft
    nLJClassGenerator.setMappingSet(emitLeftMapping);
    JExpression outIndex = JExpr.direct("outIndex");
    JExpression leftIndex = JExpr.direct("leftIndex");

    int fieldId = 0;
    int outputFieldId = 0;
    if (leftSchema != null) {
      // Set the input and output value vector references corresponding to the left batch
      for (MaterializedField field : leftSchema) {
        final TypeProtos.MajorType fieldType = field.getType();

        // Add the vector to the output container
        container.addOrGet(field);

        JVar inVV = nLJClassGenerator.declareVectorValueSetupAndMember("leftBatch",
            new TypedFieldId(fieldType, false, fieldId));
        JVar outVV = nLJClassGenerator.declareVectorValueSetupAndMember("outgoing",
            new TypedFieldId(fieldType, false, outputFieldId));

        nLJClassGenerator.getEvalBlock().add(outVV.invoke("copyFromSafe").arg(leftIndex).arg(outIndex).arg(inVV));
        nLJClassGenerator.rotateBlock();
        fieldId++;
        outputFieldId++;
      }
    }

    // generate emitRight
    fieldId = 0;
    nLJClassGenerator.setMappingSet(emitRightMapping);
    JExpression rightIndex = JExpr.direct("rightIndex");

    if (rightSchema != null) {
      // Set the input and output value vector references corresponding to the right batch
      for (MaterializedField field : rightSchema) {

        final TypeProtos.MajorType inputType = field.getType();
        TypeProtos.MajorType outputType;
        // if join type is LEFT, make sure right batch output fields data mode is optional
        if (popConfig.getJoinType() == JoinRelType.LEFT && inputType.getMode() == TypeProtos.DataMode.REQUIRED) {
          outputType = Types.overrideMode(inputType, TypeProtos.DataMode.OPTIONAL);
        } else {
          outputType = inputType;
        }

        MaterializedField newField = MaterializedField.create(field.getName(), outputType);
        container.addOrGet(newField);

        JVar inVV = nLJClassGenerator.declareVectorValueSetupAndMember("rightBatch",
            new TypedFieldId(inputType, false, fieldId));
        JVar outVV = nLJClassGenerator.declareVectorValueSetupAndMember("outgoing",
            new TypedFieldId(outputType, false, outputFieldId));
        nLJClassGenerator.getEvalBlock().add(outVV.invoke("copyFromSafe")
            .arg(rightIndex)
            .arg(outIndex)
            .arg(inVV));
        nLJClassGenerator.rotateBlock();
        fieldId++;
        outputFieldId++;
      }
    }

    try {
      return context.getImplementationClass(lateralCG);
    } catch (IOException | ClassTransformationException ex) {
      throw new SchemaChangeException("Failed while setting up generated class with new schema information", ex);
    }
  }

  /**
   * Simple method to allocate space for all the vectors in the container.
   */
  private void allocateVectors() {
    for (final VectorWrapper<?> vw : container) {
      AllocationHelper.allocateNew(vw.getValueVector(), MAX_BATCH_SIZE);
    }
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
   * Method to get left and right batch during build schema phase for {@link LateralJoinBatch}. If left batch sees a
   * failure outcome then we don't even call next on right branch, since there is no left incoming.
   * @return true if both the left/right batch was received without failure outcome.
   *         false if either of batch is received with failure outcome.
   *
   * @throws SchemaChangeException
   */
  @Override
  protected boolean prefetchFirstBatchFromBothSides() {
    // Left can get batch with zero or more records with OK_NEW_SCHEMA outcome as first batch
    leftUpstream = next(0, left);

    boolean validBatch = setBatchState(leftUpstream);

    if (validBatch) {
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

    // We should not allocate memory for all the value vectors inside output batch
    // since this is buildSchema phase and we are sending empty batches downstream
    lateralJoiner = setupWorker();

    // Set join index as invalid (-1) if the left side is empty, else set it to 0
    leftJoinIndex = (left.getRecordCount() <= 0) ? -1 : 0;
    rightJoinIndex = -1;

    // Reset the left side of the IterOutcome since for this call, OK_NEW_SCHEMA will be returned correctly
    // by buildSchema caller and we should treat the batch as received with OK outcome.
    leftUpstream = OK;
    rightUpstream = OK;
  }

  @Override
  public void close() {
    super.close();
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
  public IterOutcome getLeftOutcome() {
    return leftUpstream;
  }
}
