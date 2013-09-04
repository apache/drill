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

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.expression.visitors.AggregateChecker;
import org.apache.drill.common.expression.visitors.ConstantChecker;
import org.apache.drill.common.expression.visitors.SimpleExprVisitor;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.eval.fn.FunctionArguments;
import org.apache.drill.exec.ref.eval.fn.FunctionEvaluatorRegistry;
import org.apache.drill.exec.ref.exceptions.SetupException;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues.BooleanScalar;
import org.apache.drill.exec.ref.values.ScalarValues.DoubleScalar;
import org.apache.drill.exec.ref.values.ScalarValues.IntegerScalar;
import org.apache.drill.exec.ref.values.ScalarValues.LongScalar;
import org.apache.drill.exec.ref.values.ScalarValues.StringScalar;

public class SimpleEvaluationVisitor extends SimpleExprVisitor<BasicEvaluator>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleEvaluationVisitor.class);

  private RecordPointer record;
  private List<AggregatingEvaluator> aggregators = new ArrayList<AggregatingEvaluator>();
  
  public SimpleEvaluationVisitor(RecordPointer record) {
    super();
    this.record = record;
  }

  public List<AggregatingEvaluator> getAggregators() {
    return aggregators;
  }

  @Override
  public BasicEvaluator visitFunctionCall(FunctionCall call) {
    List<BasicEvaluator> evals = new ArrayList<BasicEvaluator>();
    boolean includesAggregates = false;
    boolean onlyConstants = true;
    for(LogicalExpression e : call){
      if(AggregateChecker.isAggregating(e)) includesAggregates = true;
      if(!ConstantChecker.onlyIncludesConstants(e)) onlyConstants = false;
      evals.add(e.accept(this, null));
    }
    FunctionArguments args = new FunctionArguments(onlyConstants, includesAggregates, evals, call);

    if(call.getDefinition().isAggregating()){
      if(args.includesAggregates()) throw new SetupException(String.format("The call for %s contains one or more arguments that also contain aggregating functions.  An aggregating function cannot contain aggregating expressions.", call.getDefinition()));
      BasicEvaluator e = FunctionEvaluatorRegistry.getEvaluator(call.getDefinition().getName(), args, record);
      if(!(e instanceof AggregatingEvaluator ) ){
        throw new SetupException(String.format("Function %s is an aggregating function.  However, the provided evaluator (%s) is not an aggregating evaluator.", call.getDefinition(), e.getClass()));
      }
      
      aggregators.add( (AggregatingEvaluator) e);
      return e;
    }else{
      BasicEvaluator e = FunctionEvaluatorRegistry.getEvaluator(call.getDefinition().getName(), args, record);
      return e;
    }
  }

  @Override
  public BasicEvaluator visitIfExpression(IfExpression ifExpr) {
    return new IfEvaluator(ifExpr, this, record);
  }

  @Override
  public BasicEvaluator visitSchemaPath(SchemaPath path) {
    return new FieldEvaluator(path, record);
  }

  @Override
  public BasicEvaluator visitLongConstant(LongExpression longExpr) {
    return new LongScalar(longExpr.getLong());
  }

  @Override
  public BasicEvaluator visitDoubleConstant(DoubleExpression dExpr) {
    return new DoubleScalar(dExpr.getDouble());
  }

  @Override
  public BasicEvaluator visitBooleanConstant(BooleanExpression e) {
    return new BooleanScalar(e.getBoolean());
  }

  @Override
  public BasicEvaluator visitQuotedStringConstant(QuotedString e) {
    return new StringScalar(e.value);
  }
  
  
  @Override
  public BasicEvaluator visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    throw new UnsupportedOperationException();
  }

  public static void main(String[] args) throws Exception {
    String expr = "if( a == 1) then 4 else 2 end";
    ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ExprParser parser = new ExprParser(tokens);
    LogicalExpression e = parser.parse().e;
    RecordPointer r = new UnbackedRecord();
    r.addField(new SchemaPath("a", ExpressionPosition.UNKNOWN), new IntegerScalar(3));
    SimpleEvaluationVisitor builder = new SimpleEvaluationVisitor(r);
    BasicEvaluator eval = e.accept(builder, null);
    DataValue v = eval.eval();
    System.out.println(v);
  }
  
}
