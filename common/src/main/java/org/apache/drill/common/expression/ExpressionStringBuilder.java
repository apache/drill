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
package org.apache.drill.common.expression;

import java.io.IOException;

import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DateExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.IntervalDayExpression;
import org.apache.drill.common.expression.ValueExpressions.IntervalYearExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.ValueExpressions.TimeExpression;
import org.apache.drill.common.expression.ValueExpressions.TimeStampExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

import com.google.common.collect.ImmutableList;

public class ExpressionStringBuilder extends AbstractExprVisitor<Void, StringBuilder, RuntimeException>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionStringBuilder.class);

  @Override
  public Void visitFunctionCall(FunctionCall call, StringBuilder sb) throws RuntimeException {
    ImmutableList<LogicalExpression> args = call.args;
    sb.append(call.getName());
    sb.append("(");
    for (int i = 0; i < args.size(); i++) {
      if (i != 0) sb.append(", ");
      args.get(i).accept(this, sb);
    }
    sb.append(") ");
    return null;
  }

  @Override
  public Void visitFunctionHolderExpression(FunctionHolderExpression holder, StringBuilder sb) throws RuntimeException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Void visitIfExpression(IfExpression ifExpr, StringBuilder sb) throws RuntimeException {
    ImmutableList<IfCondition> conditions = ifExpr.conditions;
    sb.append(" ( ");
    for(int i =0; i < conditions.size(); i++){
      IfCondition c = conditions.get(i);
      if(i !=0) sb.append(" else ");
      sb.append("if (");
      c.condition.accept(this, sb);
      sb.append(" ) then (");
      c.expression.accept(this, sb);
      sb.append(" ) ");
    }
    sb.append(" else (");
    ifExpr.elseExpression.accept(this, sb);
    sb.append(" ) ");
    sb.append(" end ");
    sb.append(" ) ");
    return null;
  }

  @Override
  public Void visitSchemaPath(SchemaPath path, StringBuilder sb) throws RuntimeException {
    sb.append(path.getPath());
    return null;
  }

  @Override
  public Void visitLongConstant(LongExpression lExpr, StringBuilder sb) throws RuntimeException {
    sb.append(lExpr.getLong());
    return null;
  }


  @Override
  public Void visitDateConstant(DateExpression lExpr, StringBuilder sb) throws RuntimeException {
    sb.append("cast( ");
    sb.append(lExpr.getDate());
    sb.append(" as DATE)");
    return null;
  }

  @Override
  public Void visitTimeConstant(TimeExpression lExpr, StringBuilder sb) throws RuntimeException {
    sb.append("cast( ");
    sb.append(lExpr.getTime());
    sb.append(" as TIME)");
    return null;
  }

  static final DateTimeFormatter TIMESTAMP_FORMAT;
  static {
    DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
    DateTimeParser optionalTime = DateTimeFormat.forPattern(" HH:mm:ss").getParser();
    DateTimeParser optionalSec = DateTimeFormat.forPattern(".SSS").getParser();

    TIMESTAMP_FORMAT = new DateTimeFormatterBuilder().append(dateFormatter).appendOptional(optionalTime).appendOptional(optionalSec).toFormatter();

  }
  
  @Override
  public Void visitTimeStampConstant(TimeStampExpression lExpr, StringBuilder sb) throws RuntimeException {
    sb.append("cast( \"");
    try {
      TIMESTAMP_FORMAT.printTo(sb, lExpr.getTimeStamp());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    sb.append(lExpr.getTimeStamp());
    sb.append(" \" as TIMESTAMP)");
    return null;
  }

  @Override
  public Void visitIntervalYearConstant(IntervalYearExpression lExpr, StringBuilder sb) throws RuntimeException {
    sb.append("cast( \"");
    sb.append(Period.years(lExpr.getIntervalYear()).toString());
    sb.append("\" as INTERVAL)");
    return null;
  }

  @Override
  public Void visitIntervalDayConstant(IntervalDayExpression lExpr, StringBuilder sb) throws RuntimeException {
    sb.append("cast( \"");
    sb.append(Period.days(lExpr.getIntervalDay()).plusMillis(lExpr.getIntervalMillis()).toString());
    sb.append("\" as INTERVAL)");
    return null;
  }

  @Override
  public Void visitDoubleConstant(DoubleExpression dExpr, StringBuilder sb) throws RuntimeException {
    sb.append(dExpr.getDouble());
    return null;
  }

  @Override
  public Void visitBooleanConstant(BooleanExpression e, StringBuilder sb) throws RuntimeException {
    sb.append(e.getBoolean());
    return null;
  }

  @Override
  public Void visitQuotedStringConstant(QuotedString e, StringBuilder sb) throws RuntimeException {
    sb.append("\"");
    sb.append(e.value);
    sb.append("\"");
    return null;
  }

  @Override
  public Void visitCastExpression(CastExpression e, StringBuilder sb) throws RuntimeException {
    MajorType mt = e.getMajorType();
    
    sb.append("cast( (");
    e.getInput().accept(this, sb);
    sb.append(" ) as ");
    sb.append(mt.getMinorType().name());
    
    switch(mt.getMinorType()){
    case FLOAT4:
    case FLOAT8:
    case INT:
    case SMALLINT:
    case BIGINT:
    case UINT1:
    case UINT2:
    case UINT4:
    case UINT8:
    case DATE:
    case TIMESTAMP:
    case TIMESTAMPTZ:
    case TIME:
    case INTERVAL:
    case INTERVALDAY:
    case INTERVALYEAR:
      // do nothing else.
      break;
    case VAR16CHAR:
    case VARBINARY:
    case VARCHAR:
    case FIXED16CHAR:
    case FIXEDBINARY:
    case FIXEDCHAR:
      // add size in parens
      sb.append("(");
      sb.append(mt.getWidth());
      sb.append(")");
      break;
    default:
      throw new UnsupportedOperationException(String.format("Unable to convert cast expression %s into string.", e));
    }
    sb.append(" )");
    return null;
  }

  @Override
  public Void visitFloatConstant(FloatExpression fExpr, StringBuilder sb) throws RuntimeException {
    sb.append(fExpr.getFloat());
    return null;
  }

  @Override
  public Void visitIntConstant(IntExpression intExpr, StringBuilder sb) throws RuntimeException {
    sb.append(intExpr.getInt());
    return null;
  }
  
  
  
  
}
