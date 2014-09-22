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

import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JVar;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.WindowPOP;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import java.io.IOException;
import java.util.List;

public class StreamingWindowFrameRecordBatch extends AbstractSingleRecordBatch<WindowPOP> {
  private StreamingWindowFramer framer;

  public StreamingWindowFrameRecordBatch(WindowPOP popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context, incoming);
  }

  @Override
  protected void setupNewSchema() throws SchemaChangeException {
    container.clear();

    try {
      this.framer = createFramer();
    } catch (ClassTransformationException | IOException ex) {
      throw new SchemaChangeException("Failed to create framer: " + ex);
    }
  }

  private void getIndex(ClassGenerator<StreamingWindowFramer> g) {
    switch (incoming.getSchema().getSelectionVectorMode()) {
      case FOUR_BYTE: {
        JVar var = g.declareClassField("sv4_", g.getModel()._ref(SelectionVector4.class));
        g.getBlock("setupInterior").assign(var, JExpr.direct("incoming").invoke("getSelectionVector4"));
        g.getBlock("getVectorIndex")._return(var.invoke("get").arg(JExpr.direct("recordIndex")));
        return;
      }
      case NONE: {
        g.getBlock("getVectorIndex")._return(JExpr.direct("recordIndex"));
        return;
      }
      case TWO_BYTE: {
        JVar var = g.declareClassField("sv2_", g.getModel()._ref(SelectionVector2.class));
        g.getBlock("setupInterior").assign(var, JExpr.direct("incoming").invoke("getSelectionVector2"));
        g.getBlock("getVectorIndex")._return(var.invoke("get").arg(JExpr.direct("recordIndex")));
        return;
      }

      default:
        throw new IllegalStateException();
    }
  }

  private StreamingWindowFramer createFramer() throws SchemaChangeException, IOException, ClassTransformationException {
    int configLength = popConfig.getAggregations().length;
    List<LogicalExpression> valueExprs = Lists.newArrayList();
    LogicalExpression[] keyExprs = new LogicalExpression[popConfig.getWithins().length];

    ErrorCollector collector = new ErrorCollectorImpl();

    for (int i = 0; i < configLength; i++) {
      NamedExpression ne = popConfig.getAggregations()[i];
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector, context.getFunctionRegistry());
      if (expr == null) {
        continue;
      }

      final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType());
      ValueVector vector = TypeHelper.getNewVector(outputField, oContext.getAllocator());
      TypedFieldId id = container.add(vector);
      valueExprs.add(new ValueVectorWriteExpression(id, expr, true));
    }

    int j = 0;
    LogicalExpression[] windowExprs = new LogicalExpression[incoming.getSchema().getFieldCount()];
    // TODO: Should transfer all existing columns instead of copy. Currently this is not easily doable because
    // we are not processing one entire batch in one iteration, so cannot simply transfer.
    for (VectorWrapper wrapper : incoming) {
      ValueVector vv = wrapper.isHyper() ? wrapper.getValueVectors()[0] : wrapper.getValueVector();
      ValueVector vector = TypeHelper.getNewVector(vv.getField(), oContext.getAllocator());
      TypedFieldId id = container.add(vector);
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(
          new ValueVectorReadExpression(new TypedFieldId(vv.getField().getType(), wrapper.isHyper(), j)),
          incoming,
          collector,
          context.getFunctionRegistry());
      windowExprs[j] = new ValueVectorWriteExpression(id, expr, true);
      j++;
    }

    for (int i = 0; i < keyExprs.length; i++) {
      NamedExpression ne = popConfig.getWithins()[i];

      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(ne.getExpr(), incoming, collector, context.getFunctionRegistry());
      if (expr == null) {
        continue;
      }

      keyExprs[i] = expr;
    }

    if (collector.hasErrors()) {
      throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
    }

    final ClassGenerator<StreamingWindowFramer> cg = CodeGenerator.getRoot(StreamingWindowFramer.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    setupIsSame(cg, keyExprs);
    setupIsSameFromBatch(cg, keyExprs);
    addRecordValues(cg, valueExprs.toArray(new LogicalExpression[valueExprs.size()]));
    outputWindowValues(cg, windowExprs);

    cg.getBlock("resetValues")._return(JExpr.TRUE);

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    getIndex(cg);
    StreamingWindowFramer agg = context.getImplementationClass(cg);
    agg.setup(
        context,
        incoming,
        this,
        StreamingWindowFrameTemplate.UNBOUNDED, StreamingWindowFrameTemplate.CURRENT_ROW
    );
    return agg;
  }

  private final GeneratorMapping IS_SAME_PREV_INTERNAL_BATCH_READ = GeneratorMapping.create("isSameFromBatch", "isSameFromBatch", null, null); // the internal batch changes each time so we need to redo setup.
  private final GeneratorMapping IS_SAME_PREV = GeneratorMapping.create("setupInterior", "isSameFromBatch", null, null);
  private final MappingSet ISA_B1 = new MappingSet("b1Index", null, "b1", null, IS_SAME_PREV_INTERNAL_BATCH_READ, IS_SAME_PREV_INTERNAL_BATCH_READ);
  private final MappingSet ISA_B2 = new MappingSet("b2Index", null, "incoming", null, IS_SAME_PREV, IS_SAME_PREV);

  private void setupIsSameFromBatch(ClassGenerator<StreamingWindowFramer> cg, LogicalExpression[] keyExprs) {
    cg.setMappingSet(ISA_B1);
    for (LogicalExpression expr : keyExprs) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      cg.setMappingSet(ISA_B1);
      ClassGenerator.HoldingContainer first = cg.addExpr(expr, false);
      cg.setMappingSet(ISA_B2);
      ClassGenerator.HoldingContainer second = cg.addExpr(expr, false);

      LogicalExpression fh = FunctionGenerationHelper.getComparator(first, second, context.getFunctionRegistry());
      ClassGenerator.HoldingContainer out = cg.addExpr(fh, false);
      cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getEvalBlock()._return(JExpr.TRUE);
  }

  private final GeneratorMapping IS_SAME = GeneratorMapping.create("setupInterior", "isSame", null, null);
  private final MappingSet IS_SAME_I1 = new MappingSet("index1", null, IS_SAME, IS_SAME);
  private final MappingSet IS_SAME_I2 = new MappingSet("index2", null, IS_SAME, IS_SAME);

  private void setupIsSame(ClassGenerator<StreamingWindowFramer> cg, LogicalExpression[] keyExprs) {
    cg.setMappingSet(IS_SAME_I1);
    for (LogicalExpression expr : keyExprs) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      cg.setMappingSet(IS_SAME_I1);
      ClassGenerator.HoldingContainer first = cg.addExpr(expr, false);
      cg.setMappingSet(IS_SAME_I2);
      ClassGenerator.HoldingContainer second = cg.addExpr(expr, false);

      LogicalExpression fh = FunctionGenerationHelper.getComparator(first, second, context.getFunctionRegistry());
      ClassGenerator.HoldingContainer out = cg.addExpr(fh, false);
      cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getEvalBlock()._return(JExpr.TRUE);
  }

  private final GeneratorMapping EVAL_INSIDE = GeneratorMapping.create("setupInterior", "addRecord", null, null);
  private final GeneratorMapping EVAL_OUTSIDE = GeneratorMapping.create("setupInterior", "outputRecordValues", "resetValues", "cleanup");
  private final MappingSet EVAL = new MappingSet("index", "outIndex", EVAL_INSIDE, EVAL_OUTSIDE, EVAL_INSIDE);

  private void addRecordValues(ClassGenerator<StreamingWindowFramer> cg, LogicalExpression[] valueExprs) {
    cg.setMappingSet(EVAL);
    for (LogicalExpression ex : valueExprs) {
      ClassGenerator.HoldingContainer hc = cg.addExpr(ex);
      cg.getBlock(ClassGenerator.BlockType.EVAL)._if(hc.getValue().eq(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getBlock(ClassGenerator.BlockType.EVAL)._return(JExpr.TRUE);
  }

  private final GeneratorMapping OUTPUT_WINDOW_VALUES = GeneratorMapping.create("setupInterior", "outputWindowValues", null, null);
  private final MappingSet WINDOW_VALUES = new MappingSet("inIndex" /* read index */, "outIndex" /* write index */, "incoming", "outgoing", OUTPUT_WINDOW_VALUES, OUTPUT_WINDOW_VALUES);

  private void outputWindowValues(ClassGenerator<StreamingWindowFramer> cg, LogicalExpression[] valueExprs) {
    cg.setMappingSet(WINDOW_VALUES);
    for (int i = 0; i < valueExprs.length; i++) {
      ClassGenerator.HoldingContainer hc = cg.addExpr(valueExprs[i]);
      cg.getEvalBlock()._if(hc.getValue().eq(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getEvalBlock()._return(JExpr.TRUE);
  }

  @Override
  protected IterOutcome doWork() {
    StreamingWindowFramer.AggOutcome out = framer.doWork();

    while (out == StreamingWindowFramer.AggOutcome.UPDATE_AGGREGATOR) {
      framer = null;
      try {
        setupNewSchema();
      } catch (SchemaChangeException e) {
        return IterOutcome.STOP;
      }
      out = framer.doWork();
    }

    if (out == StreamingWindowFramer.AggOutcome.RETURN_AND_COMPLETE) {
      done = true;
    }

    return framer.getOutcome();
  }

  @Override
  public int getRecordCount() {
    return framer.getOutputCount();
  }

  @Override
  public void cleanup() {
    if (framer != null) {
      framer.cleanup();
    }
    super.cleanup();
  }
}
