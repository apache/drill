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
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.EvaluationVisitor;
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
      case FUNCTION_ID:
        logger.debug("Function");
        return getDrillFunctionFromOptiqCall(call);
      case POSTFIX:
        logger.debug("Postfix");
        switch(call.getKind()){
        case IS_NULL:
        case IS_TRUE:
        case IS_FALSE:
        case OTHER:
          return FunctionCallFactory.createExpression(call.getOperator().getName().toLowerCase(),
              ExpressionPosition.UNKNOWN, call.getOperands().get(0).accept(this));
        }
        throw new AssertionError("todo: implement syntax " + syntax + "(" + call + ")");
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
          switch(literal.getTypeName()){
          case DECIMAL:
          case INTEGER:
            return left.getChild(((BigDecimal)literal.getValue()).intValue());
          case CHAR:
            return left.getChild(literal.getValue2().toString());
          default:
            // fall through
          }
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
      case "FLOAT": castType = Types.required(MinorType.FLOAT4); break;
      case "DOUBLE": castType = Types.required(MinorType.FLOAT8); break;
      case "DECIMAL":

          int precision = call.getType().getPrecision();
          int scale = call.getType().getScale();

          if (precision <= 9) {
              castType = TypeProtos.MajorType.newBuilder().setMinorType(MinorType.DECIMAL9).setPrecision(precision).setScale(scale).build();
          } else if (precision <= 18) {
              castType = TypeProtos.MajorType.newBuilder().setMinorType(MinorType.DECIMAL18).setPrecision(precision).setScale(scale).build();
          } else if (precision <= 28) {
              // Inject a cast to SPARSE before casting to the dense type.
              castType = TypeProtos.MajorType.newBuilder().setMinorType(MinorType.DECIMAL28SPARSE).setPrecision(precision).setScale(scale).build();
              arg = FunctionCallFactory.createCast(castType, ExpressionPosition.UNKNOWN, arg);
              castType = TypeProtos.MajorType.newBuilder().setMinorType(MinorType.DECIMAL28DENSE).setPrecision(precision).setScale(scale).build();
          } else if (precision <= 38) {
              castType = TypeProtos.MajorType.newBuilder().setMinorType(MinorType.DECIMAL38SPARSE).setPrecision(precision).setScale(scale).build();
              arg = FunctionCallFactory.createCast(castType, ExpressionPosition.UNKNOWN, arg);
              castType = TypeProtos.MajorType.newBuilder().setMinorType(MinorType.DECIMAL38DENSE).setPrecision(precision).setScale(scale).build();
          } else {
              throw new UnsupportedOperationException("Only Decimal types with precision range 0 - 38 is supported");
          }
          break;

        case "INTERVAL_YEAR_MONTH": castType = Types.required(MinorType.INTERVALYEAR); break;
        case "INTERVAL_DAY_TIME": castType = Types.required(MinorType.INTERVALDAY); break;
        case "ANY": return arg; // Type will be same as argument.
        default: castType = Types.required(MinorType.valueOf(call.getType().getSqlTypeName().getName()));
      }

      return FunctionCallFactory.createCast(castType, ExpressionPosition.UNKNOWN, arg);

    }

    private LogicalExpression getDrillFunctionFromOptiqCall(RexCall call){
      List<LogicalExpression> args = Lists.newArrayList();
      for(RexNode n : call.getOperands()){
        args.add(n.accept(this));
      }
      String functionName = call.getOperator().getName().toLowerCase();

      /* Rewrite extract functions in the following manner
       * extract(year, date '2008-2-23') ---> extractYear(date '2008-2-23')
       */
      if (functionName.equals("extract")) {

        // Assert that the first argument to extract is a QuotedString
        assert args.get(0) instanceof ValueExpressions.QuotedString;

        // Get the unit of time to be extracted
        String timeUnitStr = ((ValueExpressions.QuotedString)args.get(0)).value;

        switch (timeUnitStr){
          case ("YEAR"):
          case ("MONTH"):
          case ("DAY"):
          case ("HOUR"):
          case ("MINUTE"):
          case ("SECOND"):
            String functionPostfix = timeUnitStr.substring(0, 1).toUpperCase() + timeUnitStr.substring(1).toLowerCase();
            functionName += functionPostfix;
            return FunctionCallFactory.createExpression(functionName, args.subList(1, 2));
          default:
            throw new UnsupportedOperationException("extract function supports the following time units: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND");
        }
      }

      // Rewrite DATE_PART functions as extract functions
      if (call.getOperator().getName().equalsIgnoreCase("DATE_PART")) {

        // assert that the function has exactly two arguments
        assert args.size() == 2;

        /* Based on the first input to the date_part function we rewrite the function as the
         * appropriate extract function. For example
         * date_part('year', date '2008-2-23') ------> extractYear(date '2008-2-23')
         */
        assert args.get(0) instanceof QuotedString;

        QuotedString extractString = (QuotedString) args.get(0);
        String functionPostfix = extractString.value.substring(0, 1).toUpperCase() + extractString.value.substring(1).toLowerCase();
        return FunctionCallFactory.createExpression("extract" + functionPostfix, args.subList(1, 2));
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
        /* TODO: Enable using Decimal literals once we have more functions implemented for Decimal
         * For now continue using Double instead of decimals

        int precision = ((BigDecimal) literal.getValue()).precision();
        if (precision <= 9) {
            return ValueExpressions.getDecimal9((BigDecimal)literal.getValue());
        } else if (precision <= 18) {
            return ValueExpressions.getDecimal18((BigDecimal)literal.getValue());
        } else if (precision <= 28) {
            return ValueExpressions.getDecimal28((BigDecimal)literal.getValue());
        } else if (precision <= 38) {
            return ValueExpressions.getDecimal38((BigDecimal)literal.getValue());
        } */

        double dbl = ((BigDecimal) literal.getValue()).doubleValue();
        logger.warn("Converting exact decimal into approximate decimal.  Should be fixed once decimal is implemented.");
        return ValueExpressions.getFloat8(dbl);
      case VARCHAR:
        return ValueExpressions.getChar(((NlsString)literal.getValue()).getValue());
      case SYMBOL:
        return ValueExpressions.getChar(literal.getValue().toString());
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
