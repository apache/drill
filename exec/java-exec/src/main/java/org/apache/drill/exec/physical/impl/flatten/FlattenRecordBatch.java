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
package org.apache.drill.exec.physical.impl.flatten;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.DrillFuncHolderExpr;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.physical.config.FlattenPOP;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatchMemoryManager;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.util.record.RecordBatchStats;
import org.apache.drill.exec.util.record.RecordBatchStats.RecordBatchIOType;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import com.carrotsearch.hppc.IntHashSet;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;

// TODO - handle the case where a user tries to flatten a scalar, should just act as a project all of the columns exactly
// as they come in
public class FlattenRecordBatch extends AbstractSingleRecordBatch<FlattenPOP> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FlattenRecordBatch.class);

  private Flattener flattener;
  private List<ValueVector> allocationVectors;
  private List<ComplexWriter> complexWriters;
  private boolean hasRemainder = false;
  private int remainderIndex = 0;
  private int recordCount;
  private final FlattenMemoryManager flattenMemoryManager;

  private final Flattener.Monitor monitor = new Flattener.Monitor() {
    @Override
    public int getBufferSizeFor(int recordCount) {
      int bufferSize = 0;
      for(final ValueVector vv : allocationVectors) {
        bufferSize += vv.getBufferSizeFor(recordCount);
      }
      return bufferSize;
    }
  };

  private static final String EMPTY_STRING = "";

  private class ClassifierResult {
    public List<String> outputNames;

    private void clear() {
      if (outputNames != null) {
        outputNames.clear();
      }

      // note:  don't clear the internal maps since they have cumulative data..
    }
  }

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

  private class FlattenMemoryManager extends RecordBatchMemoryManager {

    FlattenMemoryManager(int outputBatchSize) {
      super(outputBatchSize);
    }

    @Override
    public void update() {
      // Get sizing information for the batch.
      setRecordBatchSizer(new RecordBatchSizer(incoming));

      final TypedFieldId typedFieldId = incoming.getValueVectorId(popConfig.getColumn());
      final MaterializedField field = incoming.getSchema().getColumn(typedFieldId.getFieldIds()[0]);

      // Get column size of flatten column.
      RecordBatchSizer.ColumnSize columnSize = getRecordBatchSizer().getColumn(field.getName());

      // Average rowWidth of flatten column
      final int avgRowWidthFlattenColumn = columnSize.getNetSizePerEntry();

      // Average rowWidth excluding the flatten column.
      final int avgRowWidthWithOutFlattenColumn = getRecordBatchSizer().getNetRowWidth() - avgRowWidthFlattenColumn;

      // Average rowWidth of single element in the flatten list.
      // subtract the offset vector size from column data size.
      final int avgRowWidthSingleFlattenEntry =
        RecordBatchSizer.safeDivide(columnSize.getTotalNetSize() - (getOffsetVectorWidth() * columnSize.getValueCount()),
          columnSize.getElementCount());

      // Average rowWidth of outgoing batch.
      final int avgOutgoingRowWidth = avgRowWidthWithOutFlattenColumn + avgRowWidthSingleFlattenEntry;

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

  public FlattenRecordBatch(FlattenPOP pop, RecordBatch incoming, FragmentContext context) throws OutOfMemoryException {
    super(pop, context, incoming);

    // get the output batch size from config.
    int configuredBatchSize = (int) context.getOptions().getOption(ExecConstants.OUTPUT_BATCH_SIZE_VALIDATOR);
    flattenMemoryManager = new FlattenMemoryManager(configuredBatchSize);

      RecordBatchStats.logRecordBatchStats(getRecordBatchStatsContext(),
        "configured output batch size: %d", configuredBatchSize);
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }


  @Override
  protected void killIncoming(boolean sendUpstream) {
    super.killIncoming(sendUpstream);
    hasRemainder = false;
  }


  @Override
  public IterOutcome innerNext() {
    if (hasRemainder) {
      handleRemainder();
      // Check if we are supposed to return EMIT outcome and have consumed entire batch
      return getFinalOutcome(hasRemainder);
    }
    return super.innerNext();
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    return this.container;
  }

  @SuppressWarnings("resource")
  private void setFlattenVector() {
    final TypedFieldId typedFieldId = incoming.getValueVectorId(popConfig.getColumn());
    final MaterializedField field = incoming.getSchema().getColumn(typedFieldId.getFieldIds()[0]);
    final RepeatedValueVector vector;
    final ValueVector inVV = incoming.getValueAccessorById(
        field.getValueClass(), typedFieldId.getFieldIds()).getValueVector();

    if (! (inVV instanceof RepeatedValueVector)) {
      if (incoming.getRecordCount() != 0) {
        throw UserException.unsupportedError().message("Flatten does not support inputs of non-list values.").build(logger);
      }
      //when incoming recordCount is 0, don't throw exception since the type being seen here is not solid
      logger.error("setFlattenVector cast failed and recordcount is 0, create empty vector anyway.");
      vector = new RepeatedMapVector(field, oContext.getAllocator(), null);
    } else {
      vector = RepeatedValueVector.class.cast(inVV);
    }
    flattener.setFlattenField(vector);
  }

  @Override
  protected IterOutcome doWork() {
    flattenMemoryManager.update();
    flattener.setOutputCount(flattenMemoryManager.getOutputRowCount());

    int incomingRecordCount = incoming.getRecordCount();

    if (!doAlloc(flattenMemoryManager.getOutputRowCount())) {
      outOfMemory = true;
      return IterOutcome.OUT_OF_MEMORY;
    }

    // we call this in setupSchema, but we also need to call it here so we have a reference to the appropriate vector
    // inside of the the flattener for the current batch
    setFlattenVector();

    int childCount = incomingRecordCount == 0 ? 0 : flattener.getFlattenField().getAccessor().getInnerValueCount();
    int outputRecords = childCount == 0 ? 0: flattener.flattenRecords(incomingRecordCount, 0, monitor);
    // TODO - change this to be based on the repeated vector length
    if (outputRecords < childCount) {
      setValueCount(outputRecords);
      hasRemainder = true;
      remainderIndex = outputRecords;
      this.recordCount = remainderIndex;
    } else {
      setValueCount(outputRecords);
      flattener.resetGroupIndex();
      for(VectorWrapper<?> v: incoming) {
        v.clear();
      }
      this.recordCount = outputRecords;
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }

    flattenMemoryManager.updateOutgoingStats(outputRecords);
    RecordBatchStats.logRecordBatchStats(RecordBatchIOType.OUTPUT, this, getRecordBatchStatsContext());

    // Get the final outcome based on hasRemainder since that will determine if all the incoming records were
    // consumed in current output batch or not
    return getFinalOutcome(hasRemainder);
  }

  private void handleRemainder() {
    int remainingRecordCount = flattener.getFlattenField().getAccessor().getInnerValueCount() - remainderIndex;

    // remainingRecordCount can be much higher than number of rows we will have in outgoing batch.
    // Do memory allocation only for number of rows we are going to have in the batch.
    if (!doAlloc(Math.min(remainingRecordCount, flattenMemoryManager.getOutputRowCount()))) {
      outOfMemory = true;
      return;
    }

    int projRecords = flattener.flattenRecords(remainingRecordCount, 0, monitor);
    if (projRecords < remainingRecordCount) {
      setValueCount(projRecords);
      this.recordCount = projRecords;
      remainderIndex += projRecords;
    } else {
      setValueCount(remainingRecordCount);
      hasRemainder = false;
      remainderIndex = 0;
      for (VectorWrapper<?> v : incoming) {
        v.clear();
      }
      flattener.resetGroupIndex();
      this.recordCount = remainingRecordCount;
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }

    flattenMemoryManager.updateOutgoingStats(projRecords);

  }

  public void addComplexWriter(ComplexWriter writer) {
    complexWriters.add(writer);
  }

  private boolean doAlloc(int recordCount) {

    for (ValueVector v : this.allocationVectors) {
      // This will iteratively allocate memory for nested columns underneath.
      RecordBatchSizer.ColumnSize colSize = flattenMemoryManager.getColumnSize(v.getField().getName());
      colSize.allocateVector(v, recordCount);
    }

    //Allocate vv for complexWriters.
    if (complexWriters == null) {
      return true;
    }

    for (ComplexWriter writer : complexWriters) {
      writer.allocate();
    }

    return true;
  }

  private void setValueCount(int count) {
    for (ValueVector v : allocationVectors) {
      ValueVector.Mutator m = v.getMutator();
      m.setValueCount(count);
    }

    if (complexWriters == null) {
      return;
    }

    for (ComplexWriter writer : complexWriters) {
      writer.setValueCount(count);
    }
  }

  private FieldReference getRef(NamedExpression e) {
    return e.getRef();
  }

  /**
   * The data layout is the same for the actual data within a repeated field, as it is in a scalar vector for
   * the same sql type. For example, a repeated int vector has a vector of offsets into a regular int vector to
   * represent the lists. As the data layout for the actual values in the same in the repeated vector as in the
   * scalar vector of the same type, we can avoid making individual copies for the column being flattened, and just
   * use vector copies between the inner vector of the repeated field to the resulting scalar vector from the flatten
   * operation. This is completed after we determine how many records will fit (as we will hit either a batch end, or
   * the end of one of the other vectors while we are copying the data of the other vectors alongside each new flattened
   * value coming out of the repeated field.)
   */
  @SuppressWarnings("resource")
  private TransferPair getFlattenFieldTransferPair(FieldReference reference) {
    final TypedFieldId fieldId = incoming.getValueVectorId(popConfig.getColumn());
    final Class<?> vectorClass = incoming.getSchema().getColumn(fieldId.getFieldIds()[0]).getValueClass();
    final ValueVector flattenField = incoming.getValueAccessorById(vectorClass, fieldId.getFieldIds()).getValueVector();

    TransferPair tp = null;
    if (flattenField instanceof RepeatedMapVector) {
      tp = ((RepeatedMapVector)flattenField).getTransferPairToSingleMap(reference.getAsNamePart().getName(), oContext.getAllocator());
    } else if ( !(flattenField instanceof RepeatedValueVector) ) {
      if(incoming.getRecordCount() != 0) {
        throw UserException.unsupportedError().message("Flatten does not support inputs of non-list values.").build(logger);
      }
      logger.error("Cannot cast {} to RepeatedValueVector", flattenField);
      //when incoming recordCount is 0, don't throw exception since the type being seen here is not solid
      final ValueVector vv = new RepeatedMapVector(flattenField.getField(), oContext.getAllocator(), null);
      tp = RepeatedValueVector.class.cast(vv).getTransferPair(reference.getAsNamePart().getName(), oContext.getAllocator());
    } else {
      final ValueVector vvIn = RepeatedValueVector.class.cast(flattenField).getDataVector();
      // vvIn may be null because of fast schema return for repeated list vectors
      if (vvIn != null) {
        tp = vvIn.getTransferPair(reference.getAsNamePart().getName(), oContext.getAllocator());
      }
    }
    return tp;
  }

  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    this.allocationVectors = Lists.newArrayList();
    container.clear();
    final List<NamedExpression> exprs = getExpressionList();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPair> transfers = Lists.newArrayList();

    final ClassGenerator<Flattener> cg = CodeGenerator.getRoot(Flattener.TEMPLATE_DEFINITION, context.getOptions());
    cg.getCodeGenerator().plainJavaCapable(true);
    final IntHashSet transferFieldIds = new IntHashSet();

    final NamedExpression flattenExpr = new NamedExpression(popConfig.getColumn(), new FieldReference(popConfig.getColumn()));
    final ValueVectorReadExpression vectorRead = (ValueVectorReadExpression)ExpressionTreeMaterializer.materialize(flattenExpr.getExpr(), incoming, collector, context.getFunctionRegistry(), true);
    final FieldReference fieldReference = flattenExpr.getRef();
    final TransferPair transferPair = getFlattenFieldTransferPair(fieldReference);

    if (transferPair != null) {
      final ValueVector flattenVector = transferPair.getTo();

      // checks that list has only default ValueVector and replaces resulting ValueVector to INT typed ValueVector
      if (exprs.size() == 0 && flattenVector.getField().getType().equals(Types.LATE_BIND_TYPE)) {
        final MaterializedField outputField = MaterializedField.create(fieldReference.getAsNamePart().getName(), Types.OPTIONAL_INT);
        final ValueVector vector = TypeHelper.getNewVector(outputField, oContext.getAllocator());

        container.add(vector);
      } else {
        transfers.add(transferPair);
        container.add(flattenVector);
        transferFieldIds.add(vectorRead.getFieldId().getFieldIds()[0]);
      }
    }

    logger.debug("Added transfer for project expression.");

    ClassifierResult result = new ClassifierResult();

    for (NamedExpression namedExpression : exprs) {
      result.clear();

      String outputName = getRef(namedExpression).getRootSegment().getPath();
      if (result != null && result.outputNames != null && result.outputNames.size() > 0) {
        for (int j = 0; j < result.outputNames.size(); j++) {
          if (!result.outputNames.get(j).equals(EMPTY_STRING)) {
            outputName = result.outputNames.get(j);
            break;
          }
        }
      }

      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(namedExpression.getExpr(), incoming, collector, context.getFunctionRegistry(), true);
      if (collector.hasErrors()) {
        throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
      }
      if (expr instanceof DrillFuncHolderExpr &&
          ((DrillFuncHolderExpr) expr).getHolder().isComplexWriterFuncHolder()) {
        // Need to process ComplexWriter function evaluation.
        // Lazy initialization of the list of complex writers, if not done yet.
        if (complexWriters == null) {
          complexWriters = Lists.newArrayList();
        }

        // The reference name will be passed to ComplexWriter, used as the name of the output vector from the writer.
        ((DrillFuncHolderExpr) expr).getFieldReference(namedExpression.getRef());
        cg.addExpr(expr);
      } else {
        // need to do evaluation.
        final MaterializedField outputField;
        if (expr instanceof ValueVectorReadExpression) {
          final TypedFieldId id = ValueVectorReadExpression.class.cast(expr).getFieldId();
          @SuppressWarnings("resource")
          final ValueVector incomingVector = incoming.getValueAccessorById(id.getIntermediateClass(), id.getFieldIds()).getValueVector();
          // outputField is taken from the incoming schema to avoid the loss of nested fields
          // when the first batch will be empty.
          if (incomingVector != null) {
            outputField = incomingVector.getField().clone();
          } else {
            outputField = MaterializedField.create(outputName, expr.getMajorType());
          }
        } else {
          outputField = MaterializedField.create(outputName, expr.getMajorType());
        }
        @SuppressWarnings("resource")
        final ValueVector vector = TypeHelper.getNewVector(outputField, oContext.getAllocator());
        allocationVectors.add(vector);
        TypedFieldId fid = container.add(vector);
        ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, true);
        cg.addExpr(write);

        logger.debug("Added eval for project expression.");
      }
    }

    cg.rotateBlock();
    cg.getEvalBlock()._return(JExpr.TRUE);

    container.buildSchema(SelectionVectorMode.NONE);

    try {
      this.flattener = context.getImplementationClass(cg.getCodeGenerator());
      flattener.setup(context, incoming, this, transfers);
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
    return true;
  }

  private List<NamedExpression> getExpressionList() {

    List<NamedExpression> exprs = Lists.newArrayList();
    for (MaterializedField field : incoming.getSchema()) {
      String fieldName = field.getName();
      if (fieldName.equals(popConfig.getColumn().getRootSegmentPath())) {
        continue;
      }
      exprs.add(new NamedExpression(SchemaPath.getSimplePath(fieldName), new FieldReference(fieldName)));
    }
    return exprs;
  }

  private void updateStats() {
    stats.setLongStat(Metric.INPUT_BATCH_COUNT, flattenMemoryManager.getNumIncomingBatches());
    stats.setLongStat(Metric.AVG_INPUT_BATCH_BYTES, flattenMemoryManager.getAvgInputBatchSize());
    stats.setLongStat(Metric.AVG_INPUT_ROW_BYTES, flattenMemoryManager.getAvgInputRowWidth());
    stats.setLongStat(Metric.INPUT_RECORD_COUNT, flattenMemoryManager.getTotalInputRecords());
    stats.setLongStat(Metric.OUTPUT_BATCH_COUNT, flattenMemoryManager.getNumOutgoingBatches());
    stats.setLongStat(Metric.AVG_OUTPUT_BATCH_BYTES, flattenMemoryManager.getAvgOutputBatchSize());
    stats.setLongStat(Metric.AVG_OUTPUT_ROW_BYTES, flattenMemoryManager.getAvgOutputRowWidth());
    stats.setLongStat(Metric.OUTPUT_RECORD_COUNT, flattenMemoryManager.getTotalOutputRecords());

    RecordBatchStats.logRecordBatchStats(getRecordBatchStatsContext(),
      "incoming aggregate: count : %d, avg bytes : %d,  avg row bytes : %d, record count : %d",
      flattenMemoryManager.getNumIncomingBatches(), flattenMemoryManager.getAvgInputBatchSize(),
      flattenMemoryManager.getAvgInputRowWidth(), flattenMemoryManager.getTotalInputRecords());

    RecordBatchStats.logRecordBatchStats(getRecordBatchStatsContext(),
      "outgoing aggregate: count : %d, avg bytes : %d,  avg row bytes : %d, record count : %d",
      flattenMemoryManager.getNumOutgoingBatches(), flattenMemoryManager.getAvgOutputBatchSize(),
      flattenMemoryManager.getAvgOutputRowWidth(), flattenMemoryManager.getTotalOutputRecords());
  }

  @Override
  public void close() {
    updateStats();
    super.close();
  }

  @Override
  public void dump() {
    logger.error("FlattenRecordbatch[hasRemainder={}, remainderIndex={}, recordCount={}, flattener={}, container={}]",
        hasRemainder, remainderIndex, recordCount, flattener, container);
  }
}
