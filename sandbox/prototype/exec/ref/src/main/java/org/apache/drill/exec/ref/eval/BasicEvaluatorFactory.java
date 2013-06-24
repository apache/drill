/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.ref.eval;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.expression.visitors.SimpleExprVisitor;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BooleanEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.ConnectedEvaluator;
import org.apache.drill.exec.ref.eval.fn.agg.AggregatingWrapperEvaluator;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ValueReader;

public class BasicEvaluatorFactory extends EvaluatorFactory{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicEvaluatorFactory.class);

  public BasicEvaluatorFactory(IteratorRegistry registry){
    
  }
  
  private SimpleExprVisitor<BasicEvaluator> get(RecordPointer record){
    return new SimpleEvaluationVisitor(record);
  }
  
  @Override
  public BasicEvaluator getBasicEvaluator(RecordPointer inputRecord, LogicalExpression e) {
    return e.accept(get(inputRecord), null);
  }
  

  
  @Override
  public AggregatingEvaluator getAggregatingOperator(RecordPointer record, LogicalExpression e) {
    SimpleEvaluationVisitor visitor = new SimpleEvaluationVisitor(record);
    BasicEvaluator b = e.accept(visitor, null);
    return new AggregatingWrapperEvaluator(visitor.getAggregators(), b);
  }

  @Override
  public BooleanEvaluator getBooleanEvaluator(RecordPointer record, LogicalExpression e) {
    return new BooleanEvaluatorImpl(e.accept(get(record),  null));
  }

  @Override
  public ConnectedEvaluator getConnectedEvaluator(RecordPointer record, NamedExpression ne) {
    return new ConnectedEvaluatorImpl(record, ne);
  }

  
  private class BooleanEvaluatorImpl implements BooleanEvaluator{
    private BasicEvaluator eval;
    
    public BooleanEvaluatorImpl(BasicEvaluator e){
      this.eval = e;
    }
    
    @Override
    public boolean eval() {
      return ValueReader.getBoolean(eval.eval());
    }
    
  }
  
  private class ConnectedEvaluatorImpl implements ConnectedEvaluator{
    private SchemaPath outputPath;
    private BasicEvaluator eval;
    private RecordPointer record;
    
    public ConnectedEvaluatorImpl(RecordPointer record, NamedExpression e){
      this.outputPath = e.getRef();
      this.record = record;
      this.eval = e.getExpr().accept(get(record), null);
    }

    @Override
    public void eval() {
      DataValue val = eval.eval();
      record.addField(outputPath, val);
    }
    
    
  }


}
