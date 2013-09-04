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

import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ValueReader;

public class IfEvaluator implements BasicEvaluator{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IfEvaluator.class);

  private final IfCond[] conditions;
  private final BasicEvaluator elseExpression;
  
  public IfEvaluator(IfExpression expression, ExprVisitor<BasicEvaluator, Void, RuntimeException> evalBuilder, RecordPointer record){
    this.conditions = new IfCond[expression.conditions.size()];
    for(int i =0; i < conditions.length; i++){
      conditions[i] = new IfCond(expression.conditions.get(i), evalBuilder);
    }
    elseExpression = expression.elseExpression.accept(evalBuilder, null);
  }
  
  @Override
  public DataValue eval() {
    for(int i = 0 ; i < conditions.length; i++){
      if(ValueReader.getBoolean(conditions[i].condition.eval())) return conditions[i].valueExpression.eval() ;
    }
    return elseExpression.eval();
  }
  
  public class IfCond{
    private final BasicEvaluator condition;
    private final BasicEvaluator valueExpression;

    public IfCond(IfCondition c, ExprVisitor<BasicEvaluator, Void, RuntimeException> evalBuilder){
      this.condition = c.condition.accept(evalBuilder, null);
      this.valueExpression = c.expression.accept(evalBuilder, null);
    }
    
    public boolean matches(RecordPointer r){
      return ValueReader.getBoolean(condition.eval());
    }
    
    public DataValue eval(RecordPointer r){
      return valueExpression.eval();
    }
    
    public boolean isConstant() {
      return condition.isConstant() && valueExpression.isConstant();
    }
  }

  @Override
  public boolean isConstant() {
    for(IfCond c : conditions){
      if(!c.isConstant()) return false;
    }
    return elseExpression.isConstant();
  }
  
  
}
