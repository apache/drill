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
package org.apache.drill.exec.physical.impl.filter;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Lists;

public class FilterRecordBatch extends AbstractSingleRecordBatch<Filter>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterRecordBatch.class);

  private final SelectionVector2 sv;
  private Filterer filter;
  
  public FilterRecordBatch(Filter pop, RecordBatch incoming, FragmentContext context){
    super(pop, context, incoming);
    sv = new SelectionVector2(context.getAllocator());
  }
  
  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public int getRecordCount() {
    return sv.getCount();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return sv;
  }

  @Override
  protected void doWork() {
    int recordCount = incoming.getRecordCount();
    sv.allocateNew(recordCount);
    filter.filterBatch(recordCount);
    for(VectorWrapper<?> v : container){
      ValueVector.Mutator m = v.getValueVector().getMutator();
      m.setValueCount(recordCount);
    }
  }
  
  @Override
  protected void setupNewSchema() throws SchemaChangeException {
    container.clear();
    LogicalExpression filterExpression = popConfig.getExpr();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPair> transfers = Lists.newArrayList();
    final CodeGenerator<Filterer> cg = new CodeGenerator<Filterer>(Filterer.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    
    final LogicalExpression expr = ExpressionTreeMaterializer.materialize(filterExpression, incoming, collector);
    if(collector.hasErrors()){
      throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
    }
    
    cg.addExpr(new ReturnValueExpression(expr));
    
    for(VectorWrapper<?> v : incoming){
      TransferPair pair = v.getValueVector().getTransferPair();
      container.add(pair.getTo());
      transfers.add(pair);
    }
    
    container.buildSchema(SelectionVectorMode.TWO_BYTE);
    
    try {
      TransferPair[] tx = transfers.toArray(new TransferPair[transfers.size()]);
      this.filter = context.getImplementationClass(cg);
      filter.setup(context, incoming, this, tx);
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }
  
}
