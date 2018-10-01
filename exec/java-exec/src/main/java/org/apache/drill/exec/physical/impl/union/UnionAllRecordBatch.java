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
package org.apache.drill.exec.physical.impl.union;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.calcite.util.Pair;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.UnionAll;
import org.apache.drill.exec.record.AbstractBinaryRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.JoinBatchMemoryManager;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchMemoryManager;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessibleUtilities;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.resolver.TypeCastRules;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.util.record.RecordBatchStats;
import org.apache.drill.exec.util.record.RecordBatchStats.RecordBatchIOType;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;

public class UnionAllRecordBatch extends AbstractBinaryRecordBatch<UnionAll> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionAllRecordBatch.class);

  private SchemaChangeCallBack callBack = new SchemaChangeCallBack();
  private UnionAller unionall;
  private final List<TransferPair> transfers = Lists.newArrayList();
  private List<ValueVector> allocationVectors = Lists.newArrayList();
  private int recordCount = 0;
  private UnionInputIterator unionInputIterator;

  public UnionAllRecordBatch(UnionAll config, List<RecordBatch> children, FragmentContext context) throws OutOfMemoryException {
    super(config, context, true, children.get(0), children.get(1));

    // get the output batch size from config.
    int configuredBatchSize = (int) context.getOptions().getOption(ExecConstants.OUTPUT_BATCH_SIZE_VALIDATOR);
    batchMemoryManager = new RecordBatchMemoryManager(numInputs, configuredBatchSize);

    RecordBatchStats.logRecordBatchStats(getRecordBatchStatsContext(),
      "configured output batch size: %d", configuredBatchSize);
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    left.kill(sendUpstream);
    right.kill(sendUpstream);
  }

  protected void buildSchema() throws SchemaChangeException {
    if (! prefetchFirstBatchFromBothSides()) {
      state = BatchState.DONE;
      return;
    }

    unionInputIterator = new UnionInputIterator(leftUpstream, left, rightUpstream, right);

    if (leftUpstream == IterOutcome.NONE && rightUpstream == IterOutcome.OK_NEW_SCHEMA) {
      inferOutputFieldsOneSide(right.getSchema());
    } else if (rightUpstream == IterOutcome.NONE && leftUpstream == IterOutcome.OK_NEW_SCHEMA) {
      inferOutputFieldsOneSide((left.getSchema()));
    } else if (leftUpstream == IterOutcome.OK_NEW_SCHEMA && rightUpstream == IterOutcome.OK_NEW_SCHEMA) {
      inferOutputFieldsBothSide(left.getSchema(), right.getSchema());
    }

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    VectorAccessibleUtilities.allocateVectors(container, 0);
    VectorAccessibleUtilities.setValueCount(container,0);
  }

  @Override
  public IterOutcome innerNext() {
    try {
      while (true) {
        if (!unionInputIterator.hasNext()) {
          return IterOutcome.NONE;
        }

        Pair<IterOutcome, BatchStatusWrappper> nextBatch = unionInputIterator.next();
        IterOutcome upstream = nextBatch.left;
        BatchStatusWrappper batchStatus = nextBatch.right;

        switch (upstream) {
        case NONE:
        case OUT_OF_MEMORY:
        case STOP:
          return upstream;
        case OK_NEW_SCHEMA:
          return doWork(batchStatus, true);
        case OK:
          // skip batches with same schema as the previous one yet having 0 row.
          if (batchStatus.batch.getRecordCount() == 0) {
            VectorAccessibleUtilities.clear(batchStatus.batch);
            continue;
          }
          return doWork(batchStatus, false);
        default:
          throw new IllegalStateException(String.format("Unknown state %s.", upstream));
        }
      }
    } catch (ClassTransformationException | IOException | SchemaChangeException ex) {
      context.getExecutorState().fail(ex);
      killIncoming(false);
      return IterOutcome.STOP;
    }
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }


  @SuppressWarnings("resource")
  private IterOutcome doWork(BatchStatusWrappper batchStatus, boolean newSchema) throws ClassTransformationException, IOException, SchemaChangeException {
    Preconditions.checkArgument(batchStatus.batch.getSchema().getFieldCount() == container.getSchema().getFieldCount(),
        "Input batch and output batch have different field counthas!");

    if (newSchema) {
      createUnionAller(batchStatus.batch);
    }

    // Get number of records to include in the batch.
    final int recordsToProcess = Math.min(batchMemoryManager.getOutputRowCount(), batchStatus.getRemainingRecords());

    container.zeroVectors();
    batchMemoryManager.allocateVectors(allocationVectors, recordsToProcess);
    recordCount = unionall.unionRecords(batchStatus.recordsProcessed, recordsToProcess, 0);
    VectorUtil.setValueCount(allocationVectors, recordCount);

    // save number of records processed so far in batch status.
    batchStatus.recordsProcessed += recordCount;
    batchMemoryManager.updateOutgoingStats(recordCount);

    RecordBatchStats.logRecordBatchStats(RecordBatchIOType.OUTPUT, this, getRecordBatchStatsContext());

    if (callBack.getSchemaChangedAndReset()) {
      return IterOutcome.OK_NEW_SCHEMA;
    } else {
      return IterOutcome.OK;
    }
  }

  private void createUnionAller(RecordBatch inputBatch) throws ClassTransformationException, IOException, SchemaChangeException {
    transfers.clear();
    allocationVectors.clear();

    final ClassGenerator<UnionAller> cg = CodeGenerator.getRoot(UnionAller.TEMPLATE_DEFINITION, context.getOptions());
    cg.getCodeGenerator().plainJavaCapable(true);
    // cg.getCodeGenerator().saveCodeForDebugging(true);

    int index = 0;
    for(VectorWrapper<?> vw : inputBatch) {
      ValueVector vvIn = vw.getValueVector();
      ValueVector vvOut = container.getValueVector(index).getValueVector();

      final ErrorCollector collector = new ErrorCollectorImpl();
      // According to input data names, Minortypes, Datamodes, choose to
      // transfer directly,
      // rename columns or
      // cast data types (Minortype or DataMode)
      if (container.getSchema().getColumn(index).hasSameTypeAndMode(vvIn.getField())
          && vvIn.getField().getType().getMinorType() != TypeProtos.MinorType.MAP // Per DRILL-5521, existing bug for map transfer
          ) {
        // Transfer column
        TransferPair tp = vvIn.makeTransferPair(vvOut);
        transfers.add(tp);
      } else if (vvIn.getField().getType().getMinorType() == TypeProtos.MinorType.NULL) {
        continue;
      } else { // Copy data in order to rename the column
        SchemaPath inputPath = SchemaPath.getSimplePath(vvIn.getField().getName());
        MaterializedField inField = vvIn.getField();
        MaterializedField outputField = vvOut.getField();

        LogicalExpression expr = ExpressionTreeMaterializer.materialize(inputPath, inputBatch, collector, context.getFunctionRegistry());

        if (collector.hasErrors()) {
          throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
        }

        // If the inputs' DataMode is required and the outputs' DataMode is not required
        // cast to the one with the least restriction
        if(inField.getType().getMode() == TypeProtos.DataMode.REQUIRED
            && outputField.getType().getMode() != TypeProtos.DataMode.REQUIRED) {
          expr = ExpressionTreeMaterializer.convertToNullableType(expr, inField.getType().getMinorType(), context.getFunctionRegistry(), collector);
          if (collector.hasErrors()) {
            throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
          }
        }

        // If two inputs' MinorTypes are different,
        // Insert a cast before the Union operation
        if(inField.getType().getMinorType() != outputField.getType().getMinorType()) {
          expr = ExpressionTreeMaterializer.addCastExpression(expr, outputField.getType(), context.getFunctionRegistry(), collector);
          if (collector.hasErrors()) {
            throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
          }
        }

        TypedFieldId fid = container.getValueVectorId(SchemaPath.getSimplePath(outputField.getName()));

        boolean useSetSafe = !(vvOut instanceof FixedWidthVector);
        ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, useSetSafe);
        cg.addExpr(write);

        allocationVectors.add(vvOut);
      }
      ++index;
    }

    unionall = context.getImplementationClass(cg.getCodeGenerator());
    unionall.setup(context, inputBatch, this, transfers);
  }


  // The output table's column names always follow the left table,
  // where the output type is chosen based on DRILL's implicit casting rules
  private void inferOutputFieldsBothSide(final BatchSchema leftSchema, final BatchSchema rightSchema) {
//    outputFields = Lists.newArrayList();
    final Iterator<MaterializedField> leftIter = leftSchema.iterator();
    final Iterator<MaterializedField> rightIter = rightSchema.iterator();

    int index = 1;
    while (leftIter.hasNext() && rightIter.hasNext()) {
      MaterializedField leftField  = leftIter.next();
      MaterializedField rightField = rightIter.next();

      if (leftField.hasSameTypeAndMode(rightField)) {
        TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder().setMinorType(leftField.getType().getMinorType()).setMode(leftField.getDataMode());
        builder = Types.calculateTypePrecisionAndScale(leftField.getType(), rightField.getType(), builder);
        container.addOrGet(MaterializedField.create(leftField.getName(), builder.build()), callBack);
      } else if (Types.isUntypedNull(rightField.getType())) {
        container.addOrGet(leftField, callBack);
      } else if (Types.isUntypedNull(leftField.getType())) {
        container.addOrGet(MaterializedField.create(leftField.getName(), rightField.getType()), callBack);
      } else {
        // If the output type is not the same,
        // cast the column of one of the table to a data type which is the Least Restrictive
        TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder();
        if (leftField.getType().getMinorType() == rightField.getType().getMinorType()) {
          builder.setMinorType(leftField.getType().getMinorType());
          builder = Types.calculateTypePrecisionAndScale(leftField.getType(), rightField.getType(), builder);
        } else {
          List<TypeProtos.MinorType> types = Lists.newLinkedList();
          types.add(leftField.getType().getMinorType());
          types.add(rightField.getType().getMinorType());
          TypeProtos.MinorType outputMinorType = TypeCastRules.getLeastRestrictiveType(types);
          if (outputMinorType == null) {
            throw new DrillRuntimeException("Type mismatch between " + leftField.getType().getMinorType().toString() +
                " on the left side and " + rightField.getType().getMinorType().toString() +
                " on the right side in column " + index + " of UNION ALL");
          }
          builder.setMinorType(outputMinorType);
        }

        // The output data mode should be as flexible as the more flexible one from the two input tables
        List<TypeProtos.DataMode> dataModes = Lists.newLinkedList();
        dataModes.add(leftField.getType().getMode());
        dataModes.add(rightField.getType().getMode());
        builder.setMode(TypeCastRules.getLeastRestrictiveDataMode(dataModes));

        container.addOrGet(MaterializedField.create(leftField.getName(), builder.build()), callBack);
      }
      ++index;
    }

    assert !leftIter.hasNext() && ! rightIter.hasNext() : "Mis-match of column count should have been detected when validating sqlNode at planning";
  }

  private void inferOutputFieldsOneSide(final BatchSchema schema) {
    for (MaterializedField field : schema) {
      container.addOrGet(field, callBack);
    }
  }

  private static boolean hasSameTypeAndMode(MaterializedField leftField, MaterializedField rightField) {
    return (leftField.getType().getMinorType() == rightField.getType().getMinorType())
        && (leftField.getType().getMode() == rightField.getType().getMode());
  }

  private class BatchStatusWrappper {
    boolean prefetched;
    final RecordBatch batch;
    final int inputIndex;
    final IterOutcome outcome;
    int recordsProcessed;
    int totalRecordsToProcess;

    BatchStatusWrappper(boolean prefetched, IterOutcome outcome, RecordBatch batch, int inputIndex) {
      this.prefetched = prefetched;
      this.outcome = outcome;
      this.batch = batch;
      this.inputIndex = inputIndex;
      this.totalRecordsToProcess = batch.getRecordCount();
      this.recordsProcessed = 0;
    }

    public int getRemainingRecords() {
      return (totalRecordsToProcess - recordsProcessed);
    }

  }

  private class UnionInputIterator implements Iterator<Pair<IterOutcome, BatchStatusWrappper>> {
    private Stack<BatchStatusWrappper> batchStatusStack = new Stack<>();

    UnionInputIterator(IterOutcome leftOutCome, RecordBatch left, IterOutcome rightOutCome, RecordBatch right) {
      if (rightOutCome == IterOutcome.OK_NEW_SCHEMA) {
        batchStatusStack.push(new BatchStatusWrappper(true, IterOutcome.OK_NEW_SCHEMA, right, 1));
      }

      if (leftOutCome == IterOutcome.OK_NEW_SCHEMA) {
        batchStatusStack.push(new BatchStatusWrappper(true, IterOutcome.OK_NEW_SCHEMA, left, 0));
      }
    }

    @Override
    public boolean hasNext() {
      return ! batchStatusStack.isEmpty();
    }

    @Override
    public Pair<IterOutcome, BatchStatusWrappper> next() {
      while (!batchStatusStack.isEmpty()) {
        BatchStatusWrappper topStatus = batchStatusStack.peek();

        if (topStatus.prefetched) {
          topStatus.prefetched = false;
          batchMemoryManager.update(topStatus.batch, topStatus.inputIndex);
          RecordBatchStats.logRecordBatchStats(topStatus.inputIndex == 0 ? RecordBatchIOType.INPUT_LEFT : RecordBatchIOType.INPUT_RIGHT,
            batchMemoryManager.getRecordBatchSizer(topStatus.inputIndex),
            getRecordBatchStatsContext());
          return Pair.of(topStatus.outcome, topStatus);
        } else {

          // If we have more records to process, just return the top batch.
          if (topStatus.getRemainingRecords() > 0) {
            return Pair.of(IterOutcome.OK, topStatus);
          }

          IterOutcome outcome = UnionAllRecordBatch.this.next(topStatus.inputIndex, topStatus.batch);

          switch (outcome) {
          case OK:
          case OK_NEW_SCHEMA:
            // since we just read a new batch, update memory manager and initialize batch stats.
            topStatus.recordsProcessed = 0;
            topStatus.totalRecordsToProcess = topStatus.batch.getRecordCount();
            batchMemoryManager.update(topStatus.batch, topStatus.inputIndex);
            RecordBatchStats.logRecordBatchStats(topStatus.inputIndex == 0 ? RecordBatchIOType.INPUT_LEFT : RecordBatchIOType.INPUT_RIGHT,
              batchMemoryManager.getRecordBatchSizer(topStatus.inputIndex),
              getRecordBatchStatsContext());
            return Pair.of(outcome, topStatus);
          case OUT_OF_MEMORY:
          case STOP:
            batchStatusStack.pop();
            return Pair.of(outcome, topStatus);
          case NONE:
            batchStatusStack.pop();
            if (batchStatusStack.isEmpty()) {
              return Pair.of(IterOutcome.NONE, null);
            }
            break;
          default:
            throw new IllegalStateException(String.format("Unexpected state %s", outcome));
          }
        }
      }

      throw new NoSuchElementException();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  @Override
  public void close() {
    super.close();
    updateBatchMemoryManagerStats();

    RecordBatchStats.logRecordBatchStats(getRecordBatchStatsContext(),
      "incoming aggregate left: batch count : %d, avg bytes : %d,  avg row bytes : %d, record count : %d",
      batchMemoryManager.getNumIncomingBatches(JoinBatchMemoryManager.LEFT_INDEX), batchMemoryManager.getAvgInputBatchSize(JoinBatchMemoryManager.LEFT_INDEX),
      batchMemoryManager.getAvgInputRowWidth(JoinBatchMemoryManager.LEFT_INDEX), batchMemoryManager.getTotalInputRecords(JoinBatchMemoryManager.LEFT_INDEX));

    RecordBatchStats.logRecordBatchStats(getRecordBatchStatsContext(),
      "incoming aggregate right: batch count : %d, avg bytes : %d,  avg row bytes : %d, record count : %d",
      batchMemoryManager.getNumIncomingBatches(JoinBatchMemoryManager.RIGHT_INDEX), batchMemoryManager.getAvgInputBatchSize(JoinBatchMemoryManager.RIGHT_INDEX),
      batchMemoryManager.getAvgInputRowWidth(JoinBatchMemoryManager.RIGHT_INDEX), batchMemoryManager.getTotalInputRecords(JoinBatchMemoryManager.RIGHT_INDEX));

    RecordBatchStats.logRecordBatchStats(getRecordBatchStatsContext(),
      "outgoing aggregate: batch count : %d, avg bytes : %d,  avg row bytes : %d, record count : %d",
      batchMemoryManager.getNumOutgoingBatches(), batchMemoryManager.getAvgOutputBatchSize(),
      batchMemoryManager.getAvgOutputRowWidth(), batchMemoryManager.getTotalOutputRecords());
  }

  @Override
  public void dump() {
    logger.error("UnionAllRecordBatch[container={}, left={}, right={}, leftOutcome={}, rightOutcome={}, "
            + "recordCount={}]", container, left, right, leftUpstream, rightUpstream, recordCount);
  }
}
