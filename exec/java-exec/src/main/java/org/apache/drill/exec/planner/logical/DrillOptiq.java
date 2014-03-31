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
package org.apache.drill.exec.planner.logical;

import java.math.BigDecimal;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.NullExpression;
import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexCorrelVariable;
import org.eigenbase.rex.RexDynamicParam;
import org.eigenbase.rex.RexFieldAccess;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexLocalRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexOver;
import org.eigenbase.rex.RexRangeRef;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.sql.SqlSyntax;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.NlsString;

import com.google.common.collect.Lists;

/**
 * Utilities for Drill's planner.
 */
public class DrillOptiq {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillOptiq.class);
  
  /**
   * Converts a tree of {@link RexNode} operators into a scalar expression in Drill syntax.
   */
  public static LogicalExpression toDrill(DrillParseContext context, RelNode input, RexNode expr) {
    final RexToDrill visitor = new RexToDrill(context, input);
    return expr.accept(visitor);
  }

  private static class RexToDrill extends RexVisitorImpl<LogicalExpression> {
    private final RelNode input;
    private final DrillParseContext context;
    
    RexToDrill(DrillParseContext context, RelNode input) {
      super(true);
      this.context = context;
      this.input = input;
    }

    @Override
    public LogicalExpression visitInputRef(RexInputRef inputRef) {
      final int index = inputRef.getIndex();
      final RelDataTypeField field = input.getRowType().getFieldList().get(index);
      return new FieldReference(field.getName());
    }
    
    @Override
    public LogicalExpression visitCall(RexCall call) {
      logger.debug("RexCall {}, {}", call);
      final SqlSyntax syntax = call.getOperator().getSyntax();
      switch (syntax) {
      case BINARY:
        logger.debug("Binary");
        final String funcName = call.getOperator().getName().toLowerCase();
        List<LogicalExpression> args = Lists.newArrayList();
        for(RexNode r : call.getOperands()){
          args.add(r.accept(this));
        }
        args = Lists.reverse(args);
        LogicalExpression lastArg = args.get(0);
        for(int i = 1; i < args.size(); i++){
          lastArg = FunctionCallFactory.createExpression(funcName, Lists.newArrayList(args.get(i), lastArg));
        }

        return lastArg;
      case FUNCTION:
        logger.debug("Function");
        return getDrillFunctionFromOptiqCall(call);
      case PREFIX:
        logger.debug("Prefix");
        LogicalExpression arg = call.getOperands().get(0).accept(this);
        switch(call.getKind()){
        case NOT:
          return FunctionCallFactory.createExpression(call.getOperator().getName().toLowerCase(),
            ExpressionPosition.UNKNOWN, arg);
        }
        throw new AssertionError("todo: implement syntax " + syntax + "(" + call + ")");
      case SPECIAL:
        logger.debug("Special");
        switch(call.getKind()){
        case CAST:
          return getDrillCastFunctionFromOptiq(call);
        case LIKE:
        case SIMILAR:
          return getDrillFunctionFromOptiqCall(call);
        case CASE:
          List<LogicalExpression> caseArgs = Lists.newArrayList();
          for(RexNode r : call.getOperands()){
            caseArgs.add(r.accept(this));
          }

          caseArgs = Lists.reverse(caseArgs);
          // number of arguements are always going to be odd, because
          // Optiq adds "null" for the missing else expression at the end
          assert caseArgs.size()%2 == 1;
          LogicalExpression elseExpression = caseArgs.get(0);
          for (int i=1; i<caseArgs.size(); i=i+2) {
            elseExpression = IfExpression.newBuilder()
              .setElse(elseExpression)
              .addCondition(new IfCondition(caseArgs.get(i + 1), caseArgs.get(i))).build();
          }
          return elseExpression;
        }
        
        if (call.getOperator() == SqlStdOperatorTable.ITEM) {
          SchemaPath left = (SchemaPath) call.getOperands().get(0).accept(this);
          final RexLiteral literal = (RexLiteral) call.getOperands().get(1);
          return left.getChild((String) literal.getValue2());
        }
        
        // fall through
      default:
        throw new AssertionError("todo: implement syntax " + syntax + "(" + call + ")");
      }
    }

    private LogicalExpression doUnknown(Object o){
      logger.warn("Doesn't currently support consumption of {}.", o);
      return NullExpression.INSTANCE;
    }
    @Override
    public LogicalExpression visitLocalRef(RexLocalRef localRef) {
      return doUnknown(localRef);
    }

    @Override
    public LogicalExpression visitOver(RexOver over) {
      return doUnknown(over);
    }

    @Override
    public LogicalExpression visitCorrelVariable(RexCorrelVariable correlVariable) {
      return doUnknown(correlVariable);
    }

    @Override
    public LogicalExpression visitDynamicParam(RexDynamicParam dynamicParam) {
      return doUnknown(dynamicParam);
    }

    @Override
    public LogicalExpression visitRangeRef(RexRangeRef rangeRef) {
      return doUnknown(rangeRef);
    }

    @Override
    public LogicalExpression visitFieldAccess(RexFieldAccess fieldAccess) {
      return super.visitFieldAccess(fieldAccess);
    }


    private LogicalExpression getDrillCastFunctionFromOptiq(RexCall call){
      LogicalExpression arg = call.getOperands().get(0).accept(this);
      MajorType castType = null;
      
      switch(call.getType().getSqlTypeName().getName()){
        case "VARCHAR":
        case "CHAR":
          castType = Types.required(MinorType.VARCHAR).toBuilder().setWidth(call.getType().getPrecision()).build();
          break;
      
        case "INTEGER": castType = Types.required(MinorType.INT); break;
        case "FLOAT": Types.required(MinorType.FLOAT4); break;
        case "DOUBLE": Types.required(MinorType.FLOAT8); break;
        case "DECIMAL": throw new UnsupportedOperationException("Need to add decimal.");
        case "INTERVAL_YEAR_MONTH": Types.required(MinorType.INTERVALYEAR); break;
        case "INTERVAL_DAY_TIME": Types.required(MinorType.INTERVALDAY); break;
        default: castType = Types.required(MinorType.valueOf(call.getType().getSqlTypeName().getName()));
      }
      
      return FunctionCallFactory.createCast(castType, ExpressionPosition.UNKNOWN, arg);

    }

    private LogicalExpression getDrillFunctionFromOptiqCall(RexCall call){
      List<LogicalExpression> args = Lists.newArrayList();
      for(RexNode n : call.getOperands()){
        args.add(n.accept(this));
      }

      return FunctionCallFactory.createExpression(call.getOperator().getName().toLowerCase(), args);
    }

    @Override
    public LogicalExpression visitLiteral(RexLiteral literal) {
      switch(literal.getType().getSqlTypeName()){
      case BIGINT:
        long l = ((BigDecimal) literal.getValue()).longValue();
        return ValueExpressions.getBigInt(l);
      case BOOLEAN:
        return ValueExpressions.getBit(((Boolean) literal.getValue()));
      case CHAR:
        return ValueExpressions.getChar(((NlsString)literal.getValue()).getValue());
      case DOUBLE:
        double d = ((BigDecimal) literal.getValue()).doubleValue();
        return ValueExpressions.getFloat8(d);
      case FLOAT:
        float f = ((BigDecimal) literal.getValue()).floatValue();
        return ValueExpressions.getFloat4(f);
      case INTEGER:
        int a = ((BigDecimal) literal.getValue()).intValue();
        return ValueExpressions.getInt(a);
      case DECIMAL:
        double dbl = ((BigDecimal) literal.getValue()).doubleValue();
        logger.warn("Converting exact decimal into approximate decimal.  Should be fixed once decimal is implemented.");
        return ValueExpressions.getFloat8(dbl);
      case VARCHAR:
        return ValueExpressions.getChar(((NlsString)literal.getValue()).getValue());
      case DATE:
        return (ValueExpressions.getDate((GregorianCalendar)literal.getValue()));
      case TIME:
        return (ValueExpressions.getTime((GregorianCalendar)literal.getValue()));
      case TIMESTAMP:
        return (ValueExpressions.getTimeStamp((GregorianCalendar) literal.getValue()));
      case INTERVAL_YEAR_MONTH:
        return (ValueExpressions.getIntervalYear(((BigDecimal) (literal.getValue())).intValue()));
      case INTERVAL_DAY_TIME:
        return (ValueExpressions.getIntervalDay(((BigDecimal) (literal.getValue())).longValue()));
      case NULL:
        return NullExpression.INSTANCE;
      default:
        throw new UnsupportedOperationException(String.format("Unable to convert the value of %s and type %s to a Drill constant expression.", literal, literal.getTypeName()));
      }
    }
  }
}
