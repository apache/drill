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
package org.apache.drill.exec.physical.impl.window;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JVar;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Order;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.WindowPOP;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;

/**
 * support for OVER(PARTITION BY expression1,expression2,... [ORDER BY expressionA, expressionB,...])
 *
 * Doesn't support distinct partitions: multiple window with different PARTITION BY clauses.
 */
public class WindowFrameRecordBatch extends AbstractRecordBatch<WindowPOP> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WindowFrameRecordBatch.class);

  private final RecordBatch incoming;
  private List<WindowDataBatch> batches;
  private WindowFramer framer;

  private boolean noMoreBatches;
  private BatchSchema schema;

  /**
   * Describes supported window functions and if they output FLOAT8 or BIGINT
   */
  private enum WindowFunction {
    ROW_NUMBER(false),
    RANK(false),
    DENSE_RANK(false),
    PERCENT_RANK(true),
    CUME_DIST(true),
    LEAD(false),
    LAG(false);

    private final boolean useDouble;

    WindowFunction(boolean useDouble) {
      this.useDouble = useDouble;
    }

    public TypeProtos.MajorType getMajorType() {
      return useDouble ? Types.required(TypeProtos.MinorType.FLOAT8) : Types.required(TypeProtos.MinorType.BIGINT);
    }

    /**
     * Extract the WindowFunction corresponding to the logical expression
     * @param expr logical expression
     * @return WindowFunction or null if the logical expression is not a window function
     */
    public static WindowFunction fromExpression(final LogicalExpression expr) {
      if (!(expr instanceof FunctionCall)) {
        return null;
      }

      final String name = ((FunctionCall) expr).getName();
      try {
        return WindowFunction.valueOf(name.toUpperCase());
      } catch (IllegalArgumentException e) {
        return null; // not a window function
      }
    }
  }

  public WindowFrameRecordBatch(WindowPOP popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context);
    this.incoming = incoming;
    batches = Lists.newArrayList();
  }

  /**
   * Let's assume we have the following 3 batches of data:
   * <p><pre>
   * +---------+--------+--------------+--------+
   * |   b0    |   b1   |      b2      |   b3   |
   * +----+----+--------+----+----+----+--------+
   * | p0 | p1 |   p1   | p2 | p3 | p4 |   p5   |
   * +----+----+--------+----+----+----+--------+
   * </pre></p>
   *
   * batch b0 contains partitions p0 and p1
   * batch b1 contains partition p1
   * batch b2 contains partitions p2 p3 and p4
   * batch b3 contains partition p5
   *
   * <p><pre>
   * when innerNext() is called:
   *   call next(incoming), we receive and save b0 in a list of WindowDataBatch
   *     we can't process b0 yet because we don't know if p1 has more rows upstream
   *   call next(incoming), we receive and save b1
   *     we can't process b0 yet for the same reason previously stated
   *   call next(incoming), we receive and save b2
   *   we process b0 (using the framer) and pass the container downstream
   * when innerNext() is called:
   *   we process b1 and pass the container downstream, b0 and b1 are released from memory
   * when innerNext() is called:
   *   call next(incoming), we receive and save b3
   *   we process b2 and pass the container downstream, b2 is released from memory
   * when innerNext() is called:
   *   call next(incoming) and receive NONE
   *   we process b3 and pass the container downstream, b3 is released from memory
   * when innerNext() is called:
   *  we return NONE
   * </pre></p>
   * The previous scenario applies when we don't have an ORDER BY clause, otherwise a batch can be processed
   * as soon as we reach the final peer row of the batch's last row (we find the end of the last frame of the batch).
   * </p>
   * Because we only support the default frame, we don't need to reset the aggregations until we reach the end of
   * a partition. We can safely free a batch as soon as it has been processed.
   */
  @Override
  public IterOutcome innerNext() {
    logger.trace("innerNext(), noMoreBatches = {}", noMoreBatches);

    // Short circuit if record batch has already sent all data and is done
    if (state == BatchState.DONE) {
      return IterOutcome.NONE;
    }

    // keep saving incoming batches until the first unprocessed batch can be processed, or upstream == NONE
    while (!noMoreBatches && !framer.canDoWork()) {
      IterOutcome upstream = next(incoming);
      logger.trace("next(incoming) returned {}", upstream);

      switch (upstream) {
        case NONE:
          noMoreBatches = true;
          break;
        case OUT_OF_MEMORY:
        case NOT_YET:
        case STOP:
          cleanup();
          return upstream;
        case OK_NEW_SCHEMA:
          // We don't support schema changes
          if (!incoming.getSchema().equals(schema)) {
            if (schema != null) {
              throw new UnsupportedOperationException("OVER clause doesn't currently support changing schemas.");
            }
            this.schema = incoming.getSchema();
          }
        case OK:
          if (incoming.getRecordCount() > 0) {
            batches.add(new WindowDataBatch(incoming, oContext));
          } else {
            logger.trace("incoming has 0 records, it won't be added to batches");
          }
          break;
        default:
          throw new UnsupportedOperationException("Unsupported upstream state " + upstream);
      }
    }

    if (batches.isEmpty()) {
      logger.trace("no more batches to handle, we are DONE");
      state = BatchState.DONE;
      return IterOutcome.NONE;
    }

    // process first saved batch, then release it
    try {
      framer.doWork();
    } catch (DrillException e) {
      context.fail(e);
      cleanup();
      return IterOutcome.STOP;
    }

    if (state == BatchState.FIRST) {
      state = BatchState.NOT_FIRST;
    }

    return IterOutcome.OK;
  }

  @Override
  protected void buildSchema() throws SchemaChangeException {
    logger.trace("buildSchema()");
    IterOutcome outcome = next(incoming);
    switch (outcome) {
      case NONE:
        state = BatchState.DONE;
        container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
        return;
      case STOP:
        state = BatchState.STOP;
        return;
      case OUT_OF_MEMORY:
        state = BatchState.OUT_OF_MEMORY;
        return;
    }

    try {
      framer = createFramer(incoming);
    } catch (IOException | ClassTransformationException e) {
      throw new SchemaChangeException("Exception when creating the schema", e);
    }

    if (incoming.getRecordCount() > 0) {
      batches.add(new WindowDataBatch(incoming, oContext));
    }
  }

  private ValueVectorWriteExpression materializeLeadFunction(final LogicalExpression expression, final SchemaPath path,
                                                             final VectorAccessible batch, final ErrorCollector collector) {
    // read copied value from saved batch
    final LogicalExpression source = ExpressionTreeMaterializer.materialize(expression, batch,
      collector, context.getFunctionRegistry());

    // make sure window function vector type is Nullable, because we will write a null value in the last row
    // of each partition
    TypeProtos.MajorType majorType = source.getMajorType();
    if (majorType.getMode() == TypeProtos.DataMode.REQUIRED) {
      majorType = Types.optional(majorType.getMinorType());
    }

    // add corresponding ValueVector to container
    final MaterializedField outputField = MaterializedField.create(path, majorType);
    ValueVector vv = container.addOrGet(outputField);
    vv.allocateNew();

    // write copied value into container
    final TypedFieldId id =  container.getValueVectorId(vv.getField().getPath());
    return new ValueVectorWriteExpression(id, source, true);
  }

  private ValueVectorWriteExpression materializeLagFunction(final LogicalExpression expr, final SchemaPath path,
                                                            final VectorAccessible batch, final ErrorCollector collector) {
    // read copied value from saved batch
    final LogicalExpression source = ExpressionTreeMaterializer.materialize(expr, batch,
      collector, context.getFunctionRegistry());

    // make sure window function vector type is Nullable, because we will write a null value in the first row
    // of each partition
    TypeProtos.MajorType majorType = source.getMajorType();
    if (majorType.getMode() == TypeProtos.DataMode.REQUIRED) {
      majorType = Types.optional(majorType.getMinorType());
    }

    // add corresponding ValueVector to container
    final MaterializedField outputField = MaterializedField.create(path, majorType);
    ValueVector vv = container.addOrGet(outputField);
    vv.allocateNew();

    // write copied value into container
    final TypedFieldId id = container.getValueVectorId(vv.getField().getPath());
    return new ValueVectorWriteExpression(id, source, true);
  }

  private WindowFramer createFramer(VectorAccessible batch) throws SchemaChangeException, IOException, ClassTransformationException {
    assert framer == null : "createFramer should only be called once";

    logger.trace("creating framer");

    final List<LogicalExpression> aggExprs = Lists.newArrayList();
    final Map<WindowFunction, TypedFieldId> computedExprs = Maps.newHashMap();
    final List<LogicalExpression> keyExprs = Lists.newArrayList();
    final List<LogicalExpression> orderExprs = Lists.newArrayList();
    final List<LogicalExpression> leadExprs = Lists.newArrayList();
    final List<LogicalExpression> lagExprs = Lists.newArrayList();
    final List<LogicalExpression> internalExprs = Lists.newArrayList();
    final ErrorCollector collector = new ErrorCollectorImpl();

    container.clear();

    // all existing vectors will be transferred to the outgoing container in framer.doWork()
    for (VectorWrapper wrapper : batch) {
      container.addOrGet(wrapper.getField());
    }

    // add aggregation vectors to the container, and materialize corresponding expressions
    for (final NamedExpression ne : popConfig.getAggregations()) {
      final WindowFunction wf = WindowFunction.fromExpression(ne.getExpr());

      if (wf != null) {
        final FunctionCall call = (FunctionCall) ne.getExpr();

        if (wf == WindowFunction.LEAD) {

          leadExprs.add(materializeLeadFunction(call.args.get(0), ne.getRef(), batch, collector));
        } else if (wf == WindowFunction.LAG) {

          final ValueVectorWriteExpression writeExpr = materializeLagFunction(call.args.get(0), ne.getRef(), batch, collector);
          final TypedFieldId id = writeExpr.getFieldId();

          lagExprs.add(writeExpr);
          internalExprs.add(new ValueVectorWriteExpression(id, new ValueVectorReadExpression(id), true));
        } else {

          // add corresponding ValueVector to container
          final MaterializedField outputField = MaterializedField.create(ne.getRef(), wf.getMajorType());
          ValueVector vv = container.addOrGet(outputField);
          vv.allocateNew();

          computedExprs.put(wf, container.getValueVectorId(ne.getRef()));
        }
      } else {
        // evaluate expression over saved batch
        final LogicalExpression expr = ExpressionTreeMaterializer.materialize(ne.getExpr(), batch, collector,
          context.getFunctionRegistry());

        // add corresponding ValueVector to container
        final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType());
        ValueVector vv = container.addOrGet(outputField);
        vv.allocateNew();
        TypedFieldId id = container.getValueVectorId(ne.getRef());
        aggExprs.add(new ValueVectorWriteExpression(id, expr, true));
      }
    }

    if (container.isSchemaChanged()) {
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    }

    // materialize partition by expressions
    for (final NamedExpression ne : popConfig.getWithins()) {
      keyExprs.add(
        ExpressionTreeMaterializer.materialize(ne.getExpr(), batch, collector, context.getFunctionRegistry()));
    }

    // materialize order by expressions
    for (final Order.Ordering oe : popConfig.getOrderings()) {
      orderExprs.add(
        ExpressionTreeMaterializer.materialize(oe.getExpr(), batch, collector, context.getFunctionRegistry()));
    }

    if (collector.hasErrors()) {
      throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
    }

    final WindowFramer framer = generateFramer(keyExprs, orderExprs, aggExprs, computedExprs, leadExprs, lagExprs, internalExprs);
    framer.setup(batches, container, oContext);

    return framer;
  }

  private WindowFramer generateFramer(final List<LogicalExpression> keyExprs, final List<LogicalExpression> orderExprs,
                                      final List<LogicalExpression> aggExprs,
                                      final Map<WindowFunction, TypedFieldId> computedExprs,
                                      final List<LogicalExpression> leadExprs, final List<LogicalExpression> lagExprs,
                                      final List<LogicalExpression> internalExprs) throws IOException, ClassTransformationException {
    final ClassGenerator<WindowFramer> cg = CodeGenerator.getRoot(WindowFramer.TEMPLATE_DEFINITION, context.getFunctionRegistry());

    {
      // generating framer.isSamePartition()
      final GeneratorMapping IS_SAME_PARTITION_READ = GeneratorMapping.create("isSamePartition", "isSamePartition", null, null);
      final MappingSet isaB1 = new MappingSet("b1Index", null, "b1", null, IS_SAME_PARTITION_READ, IS_SAME_PARTITION_READ);
      final MappingSet isaB2 = new MappingSet("b2Index", null, "b2", null, IS_SAME_PARTITION_READ, IS_SAME_PARTITION_READ);
      setupIsFunction(cg, keyExprs, isaB1, isaB2);
    }

    {
      // generating framer.isPeer()
      final GeneratorMapping IS_SAME_PEER_READ = GeneratorMapping.create("isPeer", "isPeer", null, null);
      final MappingSet isaP1 = new MappingSet("b1Index", null, "b1", null, IS_SAME_PEER_READ, IS_SAME_PEER_READ);
      final MappingSet isaP2 = new MappingSet("b2Index", null, "b2", null, IS_SAME_PEER_READ, IS_SAME_PEER_READ);
      setupIsFunction(cg, orderExprs, isaP1, isaP2);
    }

    {
      // generating aggregations
      final GeneratorMapping EVAL_INSIDE = GeneratorMapping.create("setupEvaluatePeer", "evaluatePeer", null, null);
      final GeneratorMapping EVAL_OUTSIDE = GeneratorMapping.create("setupOutputRow", "outputRow", "resetValues", "cleanup");
      final MappingSet eval = new MappingSet("index", "outIndex", EVAL_INSIDE, EVAL_OUTSIDE, EVAL_INSIDE);
      generateForExpressions(cg, aggExprs, eval);
    }

    {
      // generating lead copyNext
      final GeneratorMapping mapping = GeneratorMapping.create("setupCopyNext", "copyNext", null, null);
      final MappingSet eval = new MappingSet("inIndex", "outIndex", mapping, mapping);
      generateForExpressions(cg, leadExprs, eval);

    }

    {
      // generating lag copyPrev
      final GeneratorMapping mapping = GeneratorMapping.create("setupCopyPrev", "copyPrev", null, null);
      final MappingSet eval = new MappingSet("inIndex", "outIndex", mapping, mapping);
      generateForExpressions(cg, lagExprs, eval);
    }

    {
      // generating lag copyFromInternal
      final GeneratorMapping mapping = GeneratorMapping.create("setupCopyFromInternal", "copyFromInternal", null, null);
      final MappingSet eval = new MappingSet("inIndex", "outIndex", mapping, mapping);
      generateForExpressions(cg, internalExprs, eval);
    }

    generateRankingFunctions(cg, computedExprs);

    cg.getBlock("resetValues")._return(JExpr.TRUE);

    return context.getImplementationClass(cg);
  }

  /**
   * setup comparison functions isSamePartition and isPeer
   */
  private void setupIsFunction(final ClassGenerator<WindowFramer> cg, final List<LogicalExpression> exprs,
                               final MappingSet leftMapping, final MappingSet rightMapping) {
    cg.setMappingSet(leftMapping);
    for (LogicalExpression expr : exprs) {
      cg.setMappingSet(leftMapping);
      ClassGenerator.HoldingContainer first = cg.addExpr(expr, false);
      cg.setMappingSet(rightMapping);
      ClassGenerator.HoldingContainer second = cg.addExpr(expr, false);

      LogicalExpression fh =
        FunctionGenerationHelper
          .getOrderingComparatorNullsHigh(first, second, context.getFunctionRegistry());
      ClassGenerator.HoldingContainer out = cg.addExpr(fh, false);
      cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getEvalBlock()._return(JExpr.TRUE);
  }

  /**
   * generate code to write "ranking" window function values into their respective value vectors
   */
  private void generateRankingFunctions(final ClassGenerator<WindowFramer> cg, final Map<WindowFunction, TypedFieldId> functions) {
    final GeneratorMapping mapping = GeneratorMapping.create("setupOutputRow", "outputRow", "resetValues", "cleanup");
    final MappingSet eval = new MappingSet(null, "outIndex", mapping, mapping);

    cg.setMappingSet(eval);
    for (WindowFunction function : functions.keySet()) {
      final JVar vv = cg.declareVectorValueSetupAndMember(cg.getMappingSet().getOutgoing(), functions.get(function));
      final JExpression outIndex = cg.getMappingSet().getValueWriteIndex();
      JInvocation setMethod = vv.invoke("getMutator").invoke("setSafe").arg(outIndex).arg(
        JExpr.direct("partition." + function.name().toLowerCase()));
      cg.getEvalBlock().add(setMethod);
    }
  }

  private void generateForExpressions(final ClassGenerator<WindowFramer> cg, final List<LogicalExpression> expressions,
                                      final MappingSet eval) {
    cg.setMappingSet(eval);
    for (LogicalExpression expr : expressions) {
      cg.addExpr(expr);
    }
  }

  private void cleanup() {
    if (framer != null) {
      framer.cleanup();
      framer = null;
    }

    if (batches != null) {
      for (final WindowDataBatch bd : batches) {
        bd.clear();
      }
      batches = null;
    }
  }

  @Override
  public void close() {
    cleanup();
    super.close();
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  @Override
  public int getRecordCount() {
    return framer.getOutputCount();
  }
}
