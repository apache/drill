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

import com.google.common.collect.Iterables;

import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Order;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.WindowPOP;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;

import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;

/**
 * support for OVER(PARTITION BY expression1,expression2,... [ORDER BY expressionA, expressionB,...])
 *
 */
public class WindowFrameRecordBatch extends AbstractRecordBatch<WindowPOP> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WindowFrameRecordBatch.class);

  private final RecordBatch incoming;
  private List<WindowDataBatch> batches;

  private WindowFramer framer;
  private boolean hasOrderBy; // true if window definition contains an order-by clause
  private final List<WindowFunction> functions = Lists.newArrayList();

  private boolean noMoreBatches;
  private BatchSchema schema;

  private boolean shouldStop;

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

    if (shouldStop) {
      if (!noMoreBatches) {
        IterOutcome upstream = next(incoming);
        while (upstream == IterOutcome.OK || upstream == IterOutcome.OK_NEW_SCHEMA) {
          // Clear the memory for the incoming batch
          for (VectorWrapper<?> wrapper : incoming) {
            wrapper.getValueVector().clear();
          }
          upstream = next(incoming);
        }
      }

      return IterOutcome.NONE;
    }

    // keep saving incoming batches until the first unprocessed batch can be processed, or upstream == NONE
    while (!noMoreBatches && !canDoWork()) {
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

  /**
   * @return true when all window functions are ready to process the current batch (it's the first batch currently
   * held in memory)
   */
  private boolean canDoWork() {
    if (batches.size() < 2) {
      // we need at least 2 batches even when window functions only need one batch, so we can detect the end of the
      // current partition
      return false;
    }

    final VectorAccessible current = batches.get(0);
    final int currentSize = current.getRecordCount();
    final VectorAccessible last = batches.get(batches.size() - 1);
    final int lastSize = last.getRecordCount();

    final boolean partitionEndReached = !framer.isSamePartition(currentSize - 1, current, lastSize - 1, last);
    final boolean frameEndReached = partitionEndReached || !framer.isPeer(currentSize - 1, current, lastSize - 1, last);

    for (final WindowFunction function : functions) {
      if (!function.canDoWork(batches.size(), hasOrderBy, frameEndReached, partitionEndReached)) {
        return false;
      }
    }

    return true;
  }

  @Override
  protected void buildSchema() throws SchemaChangeException {
    logger.trace("buildSchema()");
    final IterOutcome outcome = next(incoming);
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

  private WindowFramer createFramer(VectorAccessible batch) throws SchemaChangeException, IOException, ClassTransformationException {
    assert framer == null : "createFramer should only be called once";

    logger.trace("creating framer");

    final List<LogicalExpression> keyExprs = Lists.newArrayList();
    final List<LogicalExpression> orderExprs = Lists.newArrayList();
    boolean requireFullPartition = false;

    container.clear();

    functions.clear();

    hasOrderBy = popConfig.getOrderings().length > 0;

    // all existing vectors will be transferred to the outgoing container in framer.doWork()
    for (final VectorWrapper<?> wrapper : batch) {
      container.addOrGet(wrapper.getField());
    }

    // add aggregation vectors to the container, and materialize corresponding expressions
    for (final NamedExpression ne : popConfig.getAggregations()) {
      if (!(ne.getExpr() instanceof FunctionCall)) {
        throw UserException.functionError()
          .message("Unsupported window function '%s'", ne.getExpr())
          .build(logger);
      }

      final FunctionCall call = (FunctionCall) ne.getExpr();
      final WindowFunction winfun = WindowFunction.fromExpression(call);
      if (winfun.materialize(ne, container, context.getFunctionRegistry())) {
        functions.add(winfun);
        requireFullPartition |= winfun.requiresFullPartition(hasOrderBy);
      }
    }

    if (container.isSchemaChanged()) {
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    }

    // materialize partition by expressions
    for (final NamedExpression ne : popConfig.getWithins()) {
      keyExprs.add(ExpressionTreeMaterializer.materializeAndCheckErrors(ne.getExpr(), batch, context.getFunctionRegistry()));
    }

    // materialize order by expressions
    for (final Order.Ordering oe : popConfig.getOrderings()) {
      orderExprs.add(ExpressionTreeMaterializer.materializeAndCheckErrors(oe.getExpr(), batch, context.getFunctionRegistry()));
    }

    final WindowFramer framer = generateFramer(keyExprs, orderExprs, functions);
    framer.setup(batches, container, oContext, requireFullPartition);

    return framer;
  }

  private WindowFramer generateFramer(final List<LogicalExpression> keyExprs, final List<LogicalExpression> orderExprs,
      final List<WindowFunction> functions) throws IOException, ClassTransformationException {
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
      // isPeer also checks if it's the same partition
      setupIsFunction(cg, Iterables.concat(keyExprs, orderExprs), isaP1, isaP2);
    }

    for (final WindowFunction function : functions) {
      function.generateCode(cg);
    }

    cg.getBlock("resetValues")._return(JExpr.TRUE);

    return context.getImplementationClass(cg);
  }

  /**
   * setup comparison functions isSamePartition and isPeer
   */
  private void setupIsFunction(final ClassGenerator<WindowFramer> cg, final Iterable<LogicalExpression> exprs,
                               final MappingSet leftMapping, final MappingSet rightMapping) {
    cg.setMappingSet(leftMapping);
    for (LogicalExpression expr : exprs) {
      if (expr == null) {
        continue;
      }

      cg.setMappingSet(leftMapping);
      ClassGenerator.HoldingContainer first = cg.addExpr(expr, false);
      cg.setMappingSet(rightMapping);
      ClassGenerator.HoldingContainer second = cg.addExpr(expr, false);

      final LogicalExpression fh =
        FunctionGenerationHelper
          .getOrderingComparatorNullsHigh(first, second, context.getFunctionRegistry());
      final ClassGenerator.HoldingContainer out = cg.addExpr(fh, false);
      cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getEvalBlock()._return(JExpr.TRUE);
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
    shouldStop = true;
    incoming.kill(sendUpstream);
  }

  @Override
  public int getRecordCount() {
    return framer.getOutputCount();
  }
}
