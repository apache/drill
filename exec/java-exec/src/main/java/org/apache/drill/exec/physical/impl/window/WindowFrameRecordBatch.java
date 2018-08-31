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
package org.apache.drill.exec.physical.impl.window;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.shaded.guava.com.google.common.collect.Iterables;

import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Order;
import org.apache.drill.exec.compile.TemplateClassDefinition;
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
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;
import org.apache.drill.exec.vector.ValueVector;

/**
 * support for OVER(PARTITION BY expression1,expression2,... [ORDER BY expressionA, expressionB,...])
 *
 */
public class WindowFrameRecordBatch extends AbstractRecordBatch<WindowPOP> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WindowFrameRecordBatch.class);

  private final RecordBatch incoming;
  private List<WindowDataBatch> batches;

  private WindowFramer[] framers;
  private boolean hasOrderBy; // true if window definition contains an order-by clause
  private final List<WindowFunction> functions = Lists.newArrayList();

  private boolean noMoreBatches; // true when downstream returns NONE
  private BatchSchema schema;

  private boolean shouldStop; // true if we received an early termination request

  public WindowFrameRecordBatch(WindowPOP popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context);
    this.incoming = incoming;
    batches = Lists.newArrayList();
  }

  /**
   * Hold incoming batches in memory until all window functions are ready to process the batch on top of the queue
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
      doWork();
    } catch (DrillException e) {
      context.getExecutorState().fail(e);
      cleanup();
      return IterOutcome.STOP;
    }

    if (state == BatchState.FIRST) {
      state = BatchState.NOT_FIRST;
    }

    return IterOutcome.OK;
  }

  private void doWork() throws DrillException {

    final WindowDataBatch current = batches.get(0);
    final int recordCount = current.getRecordCount();

    logger.trace("WindowFramer.doWork() START, num batches {}, current batch has {} rows", batches.size(), recordCount);

    // allocate outgoing vectors
    for (VectorWrapper<?> w : container) {
      w.getValueVector().allocateNew();
    }

    for (WindowFramer framer : framers) {
      framer.doWork();
    }

    // transfer "non aggregated" vectors
    for (VectorWrapper<?> vw : current) {
      ValueVector v = container.addOrGet(vw.getField());
      TransferPair tp = vw.getValueVector().makeTransferPair(v);
      tp.transfer();
    }

    container.setRecordCount(recordCount);
    for (VectorWrapper<?> v : container) {
      v.getValueVector().getMutator().setValueCount(recordCount);
    }

    // we can safely free the current batch
    current.clear();
    batches.remove(0);

    logger.trace("doWork() END");
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

    boolean partitionEndReached;
    boolean frameEndReached;
    try {
      partitionEndReached = !framers[0].isSamePartition(currentSize - 1, current, lastSize - 1, last);
      frameEndReached = partitionEndReached || !framers[0].isPeer(currentSize - 1, current, lastSize - 1, last);

      for (final WindowFunction function : functions) {
        if (!function.canDoWork(batches.size(), popConfig, frameEndReached, partitionEndReached)) {
          return false;
        }
      }
    } catch (SchemaChangeException e) {
      throw new UnsupportedOperationException(e);
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
      createFramers(incoming);
    } catch (IOException | ClassTransformationException e) {
      throw new SchemaChangeException("Exception when creating the schema", e);
    }

    if (incoming.getRecordCount() > 0) {
      batches.add(new WindowDataBatch(incoming, oContext));
    }
  }

  private void createFramers(VectorAccessible batch) throws SchemaChangeException, IOException, ClassTransformationException {
    assert framers == null : "createFramer should only be called once";

    logger.trace("creating framer(s)");

    final List<LogicalExpression> keyExprs = Lists.newArrayList();
    final List<LogicalExpression> orderExprs = Lists.newArrayList();
    boolean requireFullPartition = false;

    boolean useDefaultFrame = false; // at least one window function uses the DefaultFrameTemplate
    boolean useCustomFrame = false; // at least one window function uses the CustomFrameTemplate

    hasOrderBy = popConfig.getOrderings().size() > 0;

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
        requireFullPartition |= winfun.requiresFullPartition(popConfig);

        if (winfun.supportsCustomFrames()) {
          useCustomFrame = true;
        } else {
          useDefaultFrame = true;
        }
      }
    }

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    container.setRecordCount(0);

    // materialize partition by expressions
    for (final NamedExpression ne : popConfig.getWithins()) {
      keyExprs.add(ExpressionTreeMaterializer.materializeAndCheckErrors(ne.getExpr(), batch, context.getFunctionRegistry()));
    }

    // materialize order by expressions
    for (final Order.Ordering oe : popConfig.getOrderings()) {
      orderExprs.add(ExpressionTreeMaterializer.materializeAndCheckErrors(oe.getExpr(), batch, context.getFunctionRegistry()));
    }

    // count how many framers we need
    int numFramers = useDefaultFrame ? 1 : 0;
    numFramers += useCustomFrame ? 1 : 0;
    assert numFramers > 0 : "No framer was needed!";

    framers = new WindowFramer[numFramers];
    int index = 0;
    if (useDefaultFrame) {
      framers[index] = generateFramer(keyExprs, orderExprs, functions, false);
      framers[index].setup(batches, container, oContext, requireFullPartition, popConfig);
      index++;
    }

    if (useCustomFrame) {
      framers[index] = generateFramer(keyExprs, orderExprs, functions, true);
      framers[index].setup(batches, container, oContext, requireFullPartition, popConfig);
    }
  }

  private WindowFramer generateFramer(final List<LogicalExpression> keyExprs, final List<LogicalExpression> orderExprs,
      final List<WindowFunction> functions, boolean useCustomFrame) throws IOException, ClassTransformationException {

    TemplateClassDefinition<WindowFramer> definition = useCustomFrame ?
      WindowFramer.FRAME_TEMPLATE_DEFINITION : WindowFramer.NOFRAME_TEMPLATE_DEFINITION;
    final ClassGenerator<WindowFramer> cg = CodeGenerator.getRoot(definition, context.getOptions());

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
      // only generate code for the proper window functions
      if (function.supportsCustomFrames() == useCustomFrame) {
        function.generateCode(cg);
      }
    }

    cg.getBlock("resetValues")._return(JExpr.TRUE);
    CodeGenerator<WindowFramer> codeGen = cg.getCodeGenerator();
    codeGen.plainJavaCapable(true);
    // Uncomment out this line to debug the generated code.
//    codeGen.saveCodeForDebugging(true);

    return context.getImplementationClass(codeGen);
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
      ClassGenerator.HoldingContainer first = cg.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
      cg.setMappingSet(rightMapping);
      ClassGenerator.HoldingContainer second = cg.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);

      final LogicalExpression fh =
        FunctionGenerationHelper
          .getOrderingComparatorNullsHigh(first, second, context.getFunctionRegistry());
      final ClassGenerator.HoldingContainer out = cg.addExpr(fh, ClassGenerator.BlkCreateMode.FALSE);
      cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getEvalBlock()._return(JExpr.TRUE);
  }

  private void cleanup() {

    if (framers != null) {
      for (WindowFramer framer : framers) {
        framer.cleanup();
      }

      framers = null;
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
    return framers[0].getOutputCount();
  }

  @Override
  public void dump() {
    logger.error("WindowFrameRecordBatch[container={}, popConfig={}, framers={}, schema={}]",
        container, popConfig, Arrays.toString(framers), schema);
  }
}
