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
package org.apache.drill.exec.physical.impl.aggregate;

import java.io.IOException;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashAggregate;
import org.apache.drill.exec.physical.impl.aggregate.HashAggregator.AggOutcome;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;

import com.sun.codemodel.JExpr;
import com.sun.codemodel.JVar;

public class HashAggBatch extends AbstractRecordBatch<HashAggregate> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggBatch.class);

  private HashAggregator aggregator;
  private final RecordBatch incoming;
  private LogicalExpression[] aggrExprs;
  private TypedFieldId[] groupByOutFieldIds;
  private TypedFieldId[] aggrOutFieldIds;      // field ids for the outgoing batch

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


  public HashAggBatch(HashAggregate popConfig, RecordBatch incoming, FragmentContext context) throws ExecutionSetupException {
    super(popConfig, context);
    this.incoming = incoming;
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

    if (!createAggregator()) {
      state = BatchState.DONE;
    }
    for (VectorWrapper w : container) {
      AllocationHelper.allocatePrecomputedChildCount(w.getValueVector(), 0, 0, 0);
    }
  }

  @Override
  public IterOutcome innerNext() {

    if (aggregator.allFlushed()) {
      return IterOutcome.NONE;
    }

    if (aggregator.buildComplete() && !aggregator.allFlushed()) {
      // aggregation is complete and not all records have been output yet
      return aggregator.outputCurrentBatch();
    }

    logger.debug("Starting aggregator doWork; incoming record count = {} ", incoming.getRecordCount());

    AggOutcome out = aggregator.doWork();
    logger.debug("Aggregator response {}, records {}", out, aggregator.getOutputCount());
    switch (out) {
    case CLEANUP_AND_RETURN:
      container.zeroVectors();
      aggregator.cleanup();
      state = BatchState.DONE;
      // fall through
    case RETURN_OUTCOME:
      return aggregator.getOutcome();
    case UPDATE_AGGREGATOR:
      context.fail(UserException.unsupportedError()
        .message("Hash aggregate does not support schema changes").build(logger));
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
    logger.debug("Creating new aggregator.");
    try {
      stats.startSetup();
      this.aggregator = createAggregatorInternal();
      return true;
    } catch (SchemaChangeException | ClassTransformationException | IOException ex) {
      context.fail(ex);
      container.clear();
      incoming.kill(false);
      return false;
    } finally {
      stats.stopSetup();
    }
  }

  private HashAggregator createAggregatorInternal() throws SchemaChangeException, ClassTransformationException,
      IOException {
    CodeGenerator<HashAggregator> top =
        CodeGenerator.get(HashAggregator.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    ClassGenerator<HashAggregator> cg = top.getRoot();
    ClassGenerator<HashAggregator> cgInner = cg.getInnerGenerator("BatchHolder");

    container.clear();

    int numGroupByExprs = (popConfig.getGroupByExprs() != null) ? popConfig.getGroupByExprs().length : 0;
    int numAggrExprs = (popConfig.getAggrExprs() != null) ? popConfig.getAggrExprs().length : 0;
    aggrExprs = new LogicalExpression[numAggrExprs];
    groupByOutFieldIds = new TypedFieldId[numGroupByExprs];
    aggrOutFieldIds = new TypedFieldId[numAggrExprs];

    ErrorCollector collector = new ErrorCollectorImpl();

    int i;

    for (i = 0; i < numGroupByExprs; i++) {
      NamedExpression ne = popConfig.getGroupByExprs()[i];
      final LogicalExpression expr =
          ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector, context.getFunctionRegistry());
      if (expr == null) {
        continue;
      }

      final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType());
      ValueVector vv = TypeHelper.getNewVector(outputField, oContext.getAllocator());

      // add this group-by vector to the output container
      groupByOutFieldIds[i] = container.add(vv);
    }

    for (i = 0; i < numAggrExprs; i++) {
      NamedExpression ne = popConfig.getAggrExprs()[i];
      final LogicalExpression expr =
          ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector, context.getFunctionRegistry());

      if (collector.hasErrors()) {
        throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      }

      if (expr == null) {
        continue;
      }

      final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType());
      ValueVector vv = TypeHelper.getNewVector(outputField, oContext.getAllocator());
      aggrOutFieldIds[i] = container.add(vv);

      aggrExprs[i] = new ValueVectorWriteExpression(aggrOutFieldIds[i], expr, true);
    }

    setupUpdateAggrValues(cgInner);
    setupGetIndex(cg);
    cg.getBlock("resetValues")._return(JExpr.TRUE);

    container.buildSchema(SelectionVectorMode.NONE);
    HashAggregator agg = context.getImplementationClass(top);

    HashTableConfig htConfig =
        new HashTableConfig(context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE_KEY).num_val.intValue(),
            HashTable.DEFAULT_LOAD_FACTOR, popConfig.getGroupByExprs(), null /* no probe exprs */);

    agg.setup(popConfig, htConfig, context, this.stats,
        oContext.getAllocator(), incoming, this,
        aggrExprs,
        cgInner.getWorkspaceTypes(),
        groupByOutFieldIds,
        this.container);

    return agg;
  }

  private void setupUpdateAggrValues(ClassGenerator<HashAggregator> cg) {
    cg.setMappingSet(UpdateAggrValuesMapping);

    for (LogicalExpression aggr : aggrExprs) {
      HoldingContainer hc = cg.addExpr(aggr);
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

  @Override
  public void close() {
    if (aggregator != null) {
      aggregator.cleanup();
    }
    super.close();
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

}
