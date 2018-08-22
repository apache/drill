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
package org.apache.drill.exec.physical.impl.aggregate;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashAggregate;
import org.apache.drill.exec.physical.impl.aggregate.HashAggregator.AggOutcome;
import org.apache.drill.exec.physical.impl.common.Comparator;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.planner.physical.AggPrelBase;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchMemoryManager;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.ValueVector;

import com.sun.codemodel.JExpr;
import com.sun.codemodel.JVar;

public class HashAggBatch extends AbstractRecordBatch<HashAggregate> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggBatch.class);

  private HashAggregator aggregator;
  private RecordBatch incoming;
  private LogicalExpression[] aggrExprs;
  private TypedFieldId[] groupByOutFieldIds;
  private TypedFieldId[] aggrOutFieldIds;      // field ids for the outgoing batch
  private final List<Comparator> comparators;
  private BatchSchema incomingSchema;
  private boolean wasKilled;

  private int numGroupByExprs, numAggrExprs;

  // This map saves the mapping between outgoing column and incoming column.
  private Map<String, String> columnMapping;
  private final HashAggMemoryManager hashAggMemoryManager;

  private final GeneratorMapping UPDATE_AGGR_INSIDE =
      GeneratorMapping.create("setupInterior" /* setup method */, "updateAggrValuesInternal" /* eval method */,
          "resetValues" /* reset */, "cleanup" /* cleanup */);

  private final GeneratorMapping UPDATE_AGGR_OUTSIDE =
      GeneratorMapping.create("setupInterior" /* setup method */, "outputRecordValues" /* eval method */,
          "resetValues" /* reset */, "cleanup" /* cleanup */);

  private final MappingSet UpdateAggrValuesMapping =
      new MappingSet("incomingRowIdx" /* read index */, "outRowIdx" /* write index */,
          "htRowIdx" /* workspace index */, "incoming" /* read container */, "outgoing" /* write container */,
          "aggrValuesContainer" /* workspace container */, UPDATE_AGGR_INSIDE, UPDATE_AGGR_OUTSIDE, UPDATE_AGGR_INSIDE);

  public int getOutputRowCount() {
    return hashAggMemoryManager.getOutputRowCount();
  }

  public RecordBatchMemoryManager getRecordBatchMemoryManager() {
    return hashAggMemoryManager;
  }

  private class HashAggMemoryManager extends RecordBatchMemoryManager {
    private int valuesRowWidth = 0;

    HashAggMemoryManager(int outputBatchSize) {
      super(outputBatchSize);
    }

    @Override
    public void update() {
      update(incoming);
    }

    @Override
    public void update(RecordBatch incomingRecordBatch) {
      // Get sizing information for the batch.
      setRecordBatchSizer(new RecordBatchSizer(incomingRecordBatch));

      int fieldId = 0;
      int newOutgoingRowWidth = 0;
      for (VectorWrapper<?> w : container) {
        if (w.getValueVector() instanceof FixedWidthVector) {
          newOutgoingRowWidth += ((FixedWidthVector) w.getValueVector()).getValueWidth();
          if (fieldId >= numGroupByExprs) {
            valuesRowWidth += ((FixedWidthVector) w.getValueVector()).getValueWidth();
          }
        } else {
          int columnWidth;
          if (columnMapping.get(w.getValueVector().getField().getName()) == null) {
             columnWidth = TypeHelper.getSize(w.getField().getType());
          } else {
            RecordBatchSizer.ColumnSize columnSize = getRecordBatchSizer().getColumn(columnMapping.get(w.getValueVector().getField().getName()));
            if (columnSize == null) {
              columnWidth = TypeHelper.getSize(w.getField().getType());
            } else {
              columnWidth = columnSize.getAllocSizePerEntry();
            }
          }
          newOutgoingRowWidth += columnWidth;
          if (fieldId >= numGroupByExprs) {
            valuesRowWidth += columnWidth;
          }
        }
        fieldId++;
      }

      if (updateIfNeeded(newOutgoingRowWidth)) {
        // There is an update to outgoing row width.
        // un comment this if we want to adjust the batch row count of in flight batches.
        // To keep things simple, we are not doing this adjustment for now.
        // aggregator.adjustOutputCount(getOutputBatchSize(), getOutgoingRowWidth(), newOutgoingRowWidth);
      }

      updateIncomingStats();
      if (logger.isDebugEnabled()) {
        logger.debug("BATCH_STATS, incoming: {}", getRecordBatchSizer());
      }
    }
  }

  public HashAggBatch(HashAggregate popConfig, RecordBatch incoming, FragmentContext context) {
    super(popConfig, context);
    this.incoming = incoming;
    wasKilled = false;

    final int numGrpByExprs = popConfig.getGroupByExprs().size();
    comparators = Lists.newArrayListWithExpectedSize(numGrpByExprs);
    for (int i=0; i<numGrpByExprs; i++) {
      // nulls are equal in group by case
      comparators.add(Comparator.IS_NOT_DISTINCT_FROM);
    }

    // This operator manages its memory use. Ask for leniency
    // from the allocator to allow for slight errors due to the
    // lumpiness of vector allocations beyond our control.

    boolean allowed = oContext.getAllocator().setLenient();
    logger.debug("Config: Is allocator lenient? {}", allowed);

    // get the output batch size from config.
    int configuredBatchSize = (int) context.getOptions().getOption(ExecConstants.OUTPUT_BATCH_SIZE_VALIDATOR);

    // If needed - reduce the size to allow enough batches in the available memory
    long memAvail = oContext.getAllocator().getLimit();
    long minBatchesPerPartition = context.getOptions().getOption(ExecConstants.HASHAGG_MIN_BATCHES_PER_PARTITION_VALIDATOR);
    long minBatchesNeeded = 2 * minBatchesPerPartition; // 2 - to cover overheads, etc.
    boolean is2ndPhase = popConfig.getAggPhase() == AggPrelBase.OperatorPhase.PHASE_2of2;
    boolean fallbackEnabled = context.getOptions().getOption(ExecConstants.HASHAGG_FALLBACK_ENABLED_KEY).bool_val;
    if ( is2ndPhase && !fallbackEnabled ) {
      minBatchesNeeded *= 2;  // 2nd phase (w/o fallback) needs at least 2 partitions
    }
    if ( configuredBatchSize > memAvail / minBatchesNeeded ) { // no cast - memAvail may be bigger than max-int
      int reducedBatchSize = (int)(memAvail / minBatchesNeeded);
      logger.trace("Reducing configured batch size from: {} to: {}, due to Mem limit: {}",
        configuredBatchSize, reducedBatchSize, memAvail);
      configuredBatchSize = reducedBatchSize;
    }

    hashAggMemoryManager = new HashAggMemoryManager(configuredBatchSize);
    logger.debug("BATCH_STATS, configured output batch size: {}", configuredBatchSize);

    columnMapping = CaseInsensitiveMap.newHashMap();
  }

  @Override
  public int getRecordCount() {
    if (state == BatchState.DONE) {
      return 0;
    }
    return aggregator.getOutputCount();
  }

  @Override
  public void buildSchema() throws SchemaChangeException {
    IterOutcome outcome = next(incoming);
    switch (outcome) {
      case NONE:
        state = BatchState.DONE;
        container.buildSchema(SelectionVectorMode.NONE);
        return;
      case OUT_OF_MEMORY:
        state = BatchState.OUT_OF_MEMORY;
        return;
      case STOP:
        state = BatchState.STOP;
        return;
    }

    this.incomingSchema = incoming.getSchema();
    if (!createAggregator()) {
      state = BatchState.DONE;
    }
    for (VectorWrapper<?> w : container) {
      AllocationHelper.allocatePrecomputedChildCount(w.getValueVector(), 0, 0, 0);
    }
    if (incoming.getRecordCount() > 0) {
      hashAggMemoryManager.update();
    }
  }

  @Override
  public IterOutcome innerNext() {

    if (aggregator.allFlushed()) {
      return IterOutcome.NONE;
    }

    // if aggregation is complete and not all records have been output yet
    if (aggregator.buildComplete() ||
        // or: 1st phase need to return (not fully grouped) partial output due to memory pressure
        aggregator.earlyOutput() ||
        // or: while handling an EMIT - returning output for that section
        aggregator.handlingEmit() ) {
      // then output the next batch downstream
      HashAggregator.AggIterOutcome aggOut = aggregator.outputCurrentBatch();
      // if Batch returned, or end of data, or Emit - then return the appropriate iter outcome
      switch ( aggOut ) {
        case AGG_NONE:
          return IterOutcome.NONE;
        case AGG_OK:
          return IterOutcome.OK;
        case AGG_EMIT:
          return IterOutcome.EMIT;
        default: // i.e. RESTART
          // if RESTART - continue below with doWork() - read some spilled partition, just like reading incoming
          incoming = aggregator.getNewIncoming(); // Restart - incoming was just changed
      }
    }

    if (wasKilled) { // if kill() was called before, then finish up
      aggregator.cleanup();
      incoming.kill(false);
      return IterOutcome.NONE;
    }

    // Read and aggregate records
    // ( may need to run again if the spilled partition that was read
    //   generated new partitions that were all spilled )
    AggOutcome out;
    do {
      //
      //  Read incoming batches and process their records
      //
      out = aggregator.doWork();
    } while (out == AggOutcome.CALL_WORK_AGAIN);

    switch (out) {
    case CLEANUP_AND_RETURN:
      container.zeroVectors();
      aggregator.cleanup();
      state = BatchState.DONE;
      // fall through
    case RETURN_OUTCOME:
      return aggregator.getOutcome();

    case UPDATE_AGGREGATOR:
      context.getExecutorState().fail(UserException.unsupportedError()
          .message(SchemaChangeException.schemaChanged(
              "Hash aggregate does not support schema change",
              incomingSchema,
              incoming.getSchema()).getMessage())
          .build(logger));
      close();
      killIncoming(false);
      return IterOutcome.STOP;
    default:
      throw new IllegalStateException(String.format("Unknown state %s.", out));
    }
  }

  /**
   * Creates a new Aggregator based on the current schema. If setup fails, this method is responsible for cleaning up
   * and informing the context of the failure state, as well is informing the upstream operators.
   *
   * @return true if the aggregator was setup successfully. false if there was a failure.
   */
  private boolean createAggregator() {
    try {
      stats.startSetup();
      this.aggregator = createAggregatorInternal();
      return true;
    } catch (SchemaChangeException | ClassTransformationException | IOException ex) {
      context.getExecutorState().fail(ex);
      container.clear();
      incoming.kill(false);
      return false;
    } finally {
      stats.stopSetup();
    }
  }

  private HashAggregator createAggregatorInternal() throws SchemaChangeException, ClassTransformationException,
      IOException {
    CodeGenerator<HashAggregator> top = CodeGenerator.get(HashAggregator.TEMPLATE_DEFINITION, context.getOptions());
    ClassGenerator<HashAggregator> cg = top.getRoot();
    ClassGenerator<HashAggregator> cgInner = cg.getInnerGenerator("BatchHolder");
    top.plainJavaCapable(true);
    // Uncomment the following line to allow debugging of the template code
    // top.saveCodeForDebugging(true);
    container.clear();

    numGroupByExprs = (popConfig.getGroupByExprs() != null) ? popConfig.getGroupByExprs().size() : 0;
    numAggrExprs = (popConfig.getAggrExprs() != null) ? popConfig.getAggrExprs().size() : 0;
    aggrExprs = new LogicalExpression[numAggrExprs];
    groupByOutFieldIds = new TypedFieldId[numGroupByExprs];
    aggrOutFieldIds = new TypedFieldId[numAggrExprs];

    ErrorCollector collector = new ErrorCollectorImpl();

    int i;

    for (i = 0; i < numGroupByExprs; i++) {
      NamedExpression ne = popConfig.getGroupByExprs().get(i);
      final LogicalExpression expr =
          ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector, context.getFunctionRegistry());
      if (expr == null) {
        continue;
      }

      final MaterializedField outputField = MaterializedField.create(ne.getRef().getAsNamePart().getName(), expr.getMajorType());
      @SuppressWarnings("resource")
      ValueVector vv = TypeHelper.getNewVector(outputField, oContext.getAllocator());

      // add this group-by vector to the output container
      groupByOutFieldIds[i] = container.add(vv);
      columnMapping.put(outputField.getName(), ne.getExpr().toString().replace('`',' ').trim());
    }

    int extraNonNullColumns = 0; // each of SUM, MAX and MIN gets an extra bigint column
    for (i = 0; i < numAggrExprs; i++) {
      NamedExpression ne = popConfig.getAggrExprs().get(i);
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector, context.getFunctionRegistry());

      if (expr instanceof IfExpression) {
        throw UserException.unsupportedError(new UnsupportedOperationException("Union type not supported in aggregate functions")).build(logger);
      }

      if (collector.hasErrors()) {
        throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      }

      if (expr == null) {
        continue;
      }

      final MaterializedField outputField = MaterializedField.create(ne.getRef().getAsNamePart().getName(), expr.getMajorType());
      @SuppressWarnings("resource") ValueVector vv = TypeHelper.getNewVector(outputField, oContext.getAllocator());
      aggrOutFieldIds[i] = container.add(vv);

      aggrExprs[i] = new ValueVectorWriteExpression(aggrOutFieldIds[i], expr, true);

      if (expr instanceof FunctionHolderExpression) {
        String funcName = ((FunctionHolderExpression) expr).getName();
        if (funcName.equals("sum") || funcName.equals("max") || funcName.equals("min")) {
          extraNonNullColumns++;
        }
        if (((FunctionCall) ne.getExpr()).args.get(0) instanceof SchemaPath) {
          columnMapping.put(outputField.getName(), ((SchemaPath) ((FunctionCall) ne.getExpr()).args.get(0)).getAsNamePart().getName());
        }  else if (((FunctionCall) ne.getExpr()).args.get(0) instanceof FunctionCall) {
          FunctionCall functionCall = (FunctionCall) ((FunctionCall) ne.getExpr()).args.get(0);
          if (functionCall.args.get(0) instanceof SchemaPath) {
            columnMapping.put(outputField.getName(), ((SchemaPath) functionCall.args.get(0)).getAsNamePart().getName());
          }
        }
      } else {
        columnMapping.put(outputField.getName(), ne.getRef().getAsNamePart().getName());
      }
    }

    setupUpdateAggrValues(cgInner);
    setupGetIndex(cg);
    cg.getBlock("resetValues")._return(JExpr.TRUE);

    container.buildSchema(SelectionVectorMode.NONE);
    HashAggregator agg = context.getImplementationClass(top);

    HashTableConfig htConfig =
        // TODO - fix the validator on this option
        new HashTableConfig((int)context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE),
            HashTable.DEFAULT_LOAD_FACTOR, popConfig.getGroupByExprs(), null /* no probe exprs */, comparators);

    agg.setup(popConfig, htConfig, context, oContext, incoming, this,
        aggrExprs,
        cgInner.getWorkspaceTypes(),
        groupByOutFieldIds,
        this.container, extraNonNullColumns * 8 /* sizeof(BigInt) */);

    return agg;
  }

  private void setupUpdateAggrValues(ClassGenerator<HashAggregator> cg) {
    cg.setMappingSet(UpdateAggrValuesMapping);

    for (LogicalExpression aggr : aggrExprs) {
      cg.addExpr(aggr, ClassGenerator.BlkCreateMode.TRUE);
    }
  }

  private void setupGetIndex(ClassGenerator<HashAggregator> cg) {
    switch (incoming.getSchema().getSelectionVectorMode()) {
    case FOUR_BYTE: {
      JVar var = cg.declareClassField("sv4_", cg.getModel()._ref(SelectionVector4.class));
      cg.getBlock("doSetup").assign(var, JExpr.direct("incoming").invoke("getSelectionVector4"));
      cg.getBlock("getVectorIndex")._return(var.invoke("get").arg(JExpr.direct("recordIndex")));
      return;
    }
    case NONE: {
      cg.getBlock("getVectorIndex")._return(JExpr.direct("recordIndex"));
      return;
    }
    case TWO_BYTE: {
      JVar var = cg.declareClassField("sv2_", cg.getModel()._ref(SelectionVector2.class));
      cg.getBlock("doSetup").assign(var, JExpr.direct("incoming").invoke("getSelectionVector2"));
      cg.getBlock("getVectorIndex")._return(var.invoke("getIndex").arg(JExpr.direct("recordIndex")));
      return;
    }
    }
  }

  private void updateStats() {
    stats.setLongStat(HashAggTemplate.Metric.INPUT_BATCH_COUNT, hashAggMemoryManager.getNumIncomingBatches());
    stats.setLongStat(HashAggTemplate.Metric.AVG_INPUT_BATCH_BYTES, hashAggMemoryManager.getAvgInputBatchSize());
    stats.setLongStat(HashAggTemplate.Metric.AVG_INPUT_ROW_BYTES, hashAggMemoryManager.getAvgInputRowWidth());
    stats.setLongStat(HashAggTemplate.Metric.INPUT_RECORD_COUNT, hashAggMemoryManager.getTotalInputRecords());
    stats.setLongStat(HashAggTemplate.Metric.OUTPUT_BATCH_COUNT, hashAggMemoryManager.getNumOutgoingBatches());
    stats.setLongStat(HashAggTemplate.Metric.AVG_OUTPUT_BATCH_BYTES, hashAggMemoryManager.getAvgOutputBatchSize());
    stats.setLongStat(HashAggTemplate.Metric.AVG_OUTPUT_ROW_BYTES, hashAggMemoryManager.getAvgOutputRowWidth());
    stats.setLongStat(HashAggTemplate.Metric.OUTPUT_RECORD_COUNT, hashAggMemoryManager.getTotalOutputRecords());

    if (logger.isDebugEnabled()) {
      logger.debug("BATCH_STATS, incoming aggregate: count : {}, avg bytes : {},  avg row bytes : {}, record count : {}",
        hashAggMemoryManager.getNumIncomingBatches(), hashAggMemoryManager.getAvgInputBatchSize(),
        hashAggMemoryManager.getAvgInputRowWidth(), hashAggMemoryManager.getTotalInputRecords());

      logger.debug("BATCH_STATS, outgoing aggregate: count : {}, avg bytes : {},  avg row bytes : {}, record count : {}",
        hashAggMemoryManager.getNumOutgoingBatches(), hashAggMemoryManager.getAvgOutputBatchSize(),
        hashAggMemoryManager.getAvgOutputRowWidth(), hashAggMemoryManager.getTotalOutputRecords());
    }
  }
  @Override
  public void close() {
    if (aggregator != null) {
      aggregator.cleanup();
    }
    updateStats();
    super.close();
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    wasKilled = true;
    incoming.kill(sendUpstream);
  }
}
