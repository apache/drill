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
package org.apache.drill.exec.physical.impl.xsort.managed;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.impl.xsort.SingleBatchSorter;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.vector.CopyUtil;

import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;

/**
 * Generates and manages the data-specific classes for this operator.
 * <p>
 * Several of the code generation methods take a batch, but the methods
 * are called for many batches, and generate code only for the first one.
 * Better would be to generate code from a schema; but Drill is not set
 * up for that at present.
 */

public class OperatorCodeGenerator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorCodeGenerator.class);

  protected static final MappingSet MAIN_MAPPING = new MappingSet((String) null, null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  protected static final MappingSet LEFT_MAPPING = new MappingSet("leftIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  protected static final MappingSet RIGHT_MAPPING = new MappingSet("rightIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);

  private static final GeneratorMapping COPIER_MAPPING = new GeneratorMapping("doSetup", "doCopy", null, null);
  private static final MappingSet COPIER_MAPPING_SET = new MappingSet(COPIER_MAPPING, COPIER_MAPPING);

  private final FragmentContext context;
  @SuppressWarnings("unused")
  private BatchSchema schema;

  /**
   * A single PriorityQueueCopier instance is used for 2 purposes:
   * 1. Merge sorted batches before spilling
   * 2. Merge sorted batches when all incoming data fits in memory
   */

  private PriorityQueueCopier copier;
  private final Sort popConfig;

  /**
   * Generated sort operation used to sort each incoming batch according to
   * the sort criteria specified in the {@link ExternalSort} definition of
   * this operator.
   */

  private SingleBatchSorter sorter;

  public OperatorCodeGenerator(FragmentContext context, Sort popConfig) {
    this.context = context;
    this.popConfig = popConfig;
  }

  public void setSchema(BatchSchema schema) {
    close();
    this.schema = schema;
  }

  public void close() {
    closeCopier();
    sorter = null;
  }

  public void closeCopier() {
    if (copier == null) {
      return; }
    try {
      copier.close();
      copier = null;
    } catch (IOException e) {
      throw UserException.dataWriteError(e)
            .message("Failure while flushing spilled data")
            .build(logger);
    }
  }

  public PriorityQueueCopier getCopier(VectorAccessible batch) {
    if (copier == null) {
      copier = generateCopier(batch);
    }
    return copier;
  }

  private PriorityQueueCopier generateCopier(VectorAccessible batch) {
    // Generate the copier code and obtain the resulting class

    CodeGenerator<PriorityQueueCopier> cg = CodeGenerator.get(PriorityQueueCopier.TEMPLATE_DEFINITION, context.getFunctionRegistry(), context.getOptions());
    ClassGenerator<PriorityQueueCopier> g = cg.getRoot();
    cg.plainJavaCapable(true);
    // Uncomment out this line to debug the generated code.
//  cg.saveCodeForDebugging(true);

    generateComparisons(g, batch);

    g.setMappingSet(COPIER_MAPPING_SET);
    CopyUtil.generateCopies(g, batch, true);
    g.setMappingSet(MAIN_MAPPING);
    return getInstance(cg);
  }

  public MSorter createNewMSorter(VectorAccessible batch) {
    return createNewMSorter(popConfig.getOrderings(), batch, MAIN_MAPPING, LEFT_MAPPING, RIGHT_MAPPING);
  }

  private MSorter createNewMSorter(List<Ordering> orderings, VectorAccessible batch, MappingSet mainMapping, MappingSet leftMapping, MappingSet rightMapping) {
    CodeGenerator<MSorter> cg = CodeGenerator.get(MSorter.TEMPLATE_DEFINITION, context.getFunctionRegistry(), context.getOptions());
    cg.plainJavaCapable(true);

    // Uncomment out this line to debug the generated code.
//  cg.saveCodeForDebugging(true);
    ClassGenerator<MSorter> g = cg.getRoot();
    g.setMappingSet(mainMapping);

    for (Ordering od : orderings) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      ErrorCollector collector = new ErrorCollectorImpl();
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector, context.getFunctionRegistry());
      if (collector.hasErrors()) {
        throw UserException.unsupportedError()
              .message("Failure while materializing expression. " + collector.toErrorString())
              .build(logger);
      }
      g.setMappingSet(leftMapping);
      HoldingContainer left = g.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
      g.setMappingSet(rightMapping);
      HoldingContainer right = g.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
      g.setMappingSet(mainMapping);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh =
          FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right,
                                                         context.getFunctionRegistry());
      HoldingContainer out = g.addExpr(fh, ClassGenerator.BlkCreateMode.FALSE);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.ASCENDING) {
        jc._then()._return(out.getValue());
      }else{
        jc._then()._return(out.getValue().minus());
      }
      g.rotateBlock();
    }

    g.rotateBlock();
    g.getEvalBlock()._return(JExpr.lit(0));

    return getInstance(cg);
  }

  public SingleBatchSorter getSorter(VectorAccessible batch) {
    if (sorter == null) {
      sorter = createNewSorter(batch);
    }
    return sorter;
  }

  private SingleBatchSorter createNewSorter(VectorAccessible batch) {
    CodeGenerator<SingleBatchSorter> cg = CodeGenerator.get(
        SingleBatchSorter.TEMPLATE_DEFINITION, context.getFunctionRegistry(),
        context.getOptions());
    ClassGenerator<SingleBatchSorter> g = cg.getRoot();
    cg.plainJavaCapable(true);
    // Uncomment out this line to debug the generated code.
//  cg.saveCodeForDebugging(true);

    generateComparisons(g, batch);
    return getInstance(cg);
  }

  private <T> T getInstance(CodeGenerator<T> cg) {
    try {
      return context.getImplementationClass(cg);
    } catch (ClassTransformationException e) {
      throw UserException.unsupportedError(e)
            .message("Code generation error - likely code error.")
            .build(logger);
    } catch (IOException e) {
      throw UserException.resourceError(e)
            .message("IO Error during code generation.")
            .build(logger);
    }
  }

  protected void generateComparisons(ClassGenerator<?> g, VectorAccessible batch)  {
    g.setMappingSet(MAIN_MAPPING);

    for (Ordering od : popConfig.getOrderings()) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      ErrorCollector collector = new ErrorCollectorImpl();
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector, context.getFunctionRegistry());
      if (collector.hasErrors()) {
        throw UserException.unsupportedError()
              .message("Failure while materializing expression. " + collector.toErrorString())
              .build(logger);
      }
      g.setMappingSet(LEFT_MAPPING);
      HoldingContainer left = g.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
      g.setMappingSet(RIGHT_MAPPING);
      HoldingContainer right = g.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
      g.setMappingSet(MAIN_MAPPING);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh =
          FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right,
                                                         context.getFunctionRegistry());
      HoldingContainer out = g.addExpr(fh, ClassGenerator.BlkCreateMode.FALSE);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.ASCENDING) {
        jc._then()._return(out.getValue());
      }else{
        jc._then()._return(out.getValue().minus());
      }
      g.rotateBlock();
    }

    g.rotateBlock();
    g.getEvalBlock()._return(JExpr.lit(0));
  }
}
