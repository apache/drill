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
package org.apache.drill.exec.physical.impl.sort;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.defs.OrderDef;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.Order.Direction;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.CodeGenerator.HoldingContainer;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.HoldingContainerExpression;
import org.apache.drill.exec.expr.fn.impl.ComparatorFunctions;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import com.google.common.collect.ImmutableList;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;

public class SortBatch extends AbstractRecordBatch<Sort> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SortBatch.class);

  public final MappingSet MAIN_MAPPING = new MappingSet( (String) null, null, CodeGenerator.DEFAULT_SCALAR_MAP, CodeGenerator.DEFAULT_SCALAR_MAP);
  public final MappingSet LEFT_MAPPING = new MappingSet("leftIndex", null, CodeGenerator.DEFAULT_SCALAR_MAP, CodeGenerator.DEFAULT_SCALAR_MAP);
  public final MappingSet RIGHT_MAPPING = new MappingSet("rightIndex", null, CodeGenerator.DEFAULT_SCALAR_MAP, CodeGenerator.DEFAULT_SCALAR_MAP);

  private static long MAX_SORT_BYTES = 8l * 1024 * 1024 * 1024;

  private final RecordBatch incoming;
  private SortRecordBatchBuilder builder;
  private SelectionVector4 sv4;
  private Sorter sorter;
  private BatchSchema schema;
  
  public SortBatch(Sort popConfig, FragmentContext context, RecordBatch incoming) {
    super(popConfig, context);
    this.incoming = incoming;
  }

  @Override
  public int getRecordCount() {
    return sv4.getCount();
  }

  @Override
  public void kill() {
    incoming.kill();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return sv4;
  }

  
  
  @Override
  protected void cleanup() {
    super.cleanup();
    container.zeroVectors();;
    sv4.clear();
  }

  @Override
  public IterOutcome next() {
    if(builder != null){
      if(sv4.next()){
        return IterOutcome.OK;
      }else{
        return IterOutcome.NONE;
      }
    }
    
    
    try{
      outer: while (true) {
        IterOutcome upstream = incoming.next();
        switch (upstream) {
        case NONE:
          break outer;
        case NOT_YET:
        case STOP:
          container.zeroVectors();
          return upstream;
        case OK_NEW_SCHEMA:
          // only change in the case that the schema truly changes.  Artificial schema changes are ignored.
          if(!incoming.getSchema().equals(schema)){
            if (builder != null) throw new UnsupportedOperationException("Sort doesn't currently support sorts with changing schemas.");
            builder = new SortRecordBatchBuilder(context.getAllocator(), MAX_SORT_BYTES, container);
            this.schema = incoming.getSchema();
          }
          // fall through.
        case OK:
          if(!builder.add(incoming)){
            throw new UnsupportedOperationException("Sort doesn't currently support doing an external sort.");
          };
          break;
        default:
          throw new UnsupportedOperationException();
        }
      }
      
      if (builder == null)
        // builder may be null at this point if the first incoming batch is empty
        return IterOutcome.NONE;

      builder.build(context);
      sv4 = builder.getSv4();

      sorter = createNewSorter();
      sorter.setup(context, this.getSelectionVector4(), this.container);
      long t1 = System.nanoTime();
      sorter.sort(sv4, container);
      logger.debug("Sorted {} records in {} micros.", sv4.getTotalCount(), (System.nanoTime() - t1)/1000);
      
      return IterOutcome.OK_NEW_SCHEMA;
      
    }catch(SchemaChangeException | ClassTransformationException | IOException ex){
      kill();
      logger.error("Failure during query", ex);
      context.fail(ex);
      return IterOutcome.STOP;
    }
  }

  private Sorter createNewSorter() throws ClassTransformationException, IOException, SchemaChangeException {
    return createNewSorter(this.context, this.popConfig.getOrderings(), this, MAIN_MAPPING, LEFT_MAPPING, RIGHT_MAPPING);
  }

  public static Sorter createNewSorter(FragmentContext context, List<OrderDef> orderings, VectorAccessible batch) throws ClassTransformationException, IOException, SchemaChangeException {
    final MappingSet mainMapping = new MappingSet( (String) null, null, CodeGenerator.DEFAULT_SCALAR_MAP, CodeGenerator.DEFAULT_SCALAR_MAP);
    final MappingSet leftMapping = new MappingSet("leftIndex", null, CodeGenerator.DEFAULT_SCALAR_MAP, CodeGenerator.DEFAULT_SCALAR_MAP);
    final MappingSet rightMapping = new MappingSet("rightIndex", null, CodeGenerator.DEFAULT_SCALAR_MAP, CodeGenerator.DEFAULT_SCALAR_MAP);
    return createNewSorter(context, orderings, batch, mainMapping, leftMapping, rightMapping);
  }
  
  public static Sorter createNewSorter(FragmentContext context, List<OrderDef> orderings, VectorAccessible batch, MappingSet mainMapping, MappingSet leftMapping, MappingSet rightMapping)
          throws ClassTransformationException, IOException, SchemaChangeException{
    CodeGenerator<Sorter> g = new CodeGenerator<Sorter>(Sorter.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    g.setMappingSet(mainMapping);
    
    for(OrderDef od : orderings){
      // first, we rewrite the evaluation stack for each side of the comparison.
      ErrorCollector collector = new ErrorCollectorImpl(); 
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector);
      if(collector.hasErrors()) throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      g.setMappingSet(leftMapping);
      HoldingContainer left = g.addExpr(expr, false);
      g.setMappingSet(rightMapping);
      HoldingContainer right = g.addExpr(expr, false);
      g.setMappingSet(mainMapping);
      
      // next we wrap the two comparison sides and add the expression block for the comparison.
      FunctionCall f = new FunctionCall(ComparatorFunctions.COMPARE_TO, ImmutableList.of((LogicalExpression) new HoldingContainerExpression(left), new HoldingContainerExpression(right)), ExpressionPosition.UNKNOWN);
      HoldingContainer out = g.addExpr(f, false);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));
      
      //TODO: is this the right order...
      if(od.getDirection() == Direction.ASC){
        jc._then()._return(out.getValue());
      }else{
        jc._then()._return(out.getValue().minus());
      }
    }
    
    g.getEvalBlock()._return(JExpr.lit(0));
    
    return context.getImplementationClass(g);


  }
  
  @Override
  public WritableBatch getWritableBatch() {
    throw new UnsupportedOperationException("A sort batch is not writable.");
  }

  @Override
  protected void killIncoming() {
    incoming.kill();
  }

  
  

}
