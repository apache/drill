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
package org.apache.drill.exec.planner.logical;

import java.math.BigDecimal;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.NullExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.TypedNullConstant;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.NlsString;

import com.google.common.collect.Lists;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.work.ExecErrorConstants;

/**
 * Utilities for Drill's planner.
 */
public class DrillOptiq {
  public static final String UNSUPPORTED_REX_NODE_ERROR = "Cannot convert RexNode to equivalent Drill expression. ";
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillOptiq.class);

  /**
   * Converts a tree of {@link RexNode} operators into a scalar expression in Drill syntax using one input.
   *
   * @param context parse context which contains planner settings
   * @param input data input
   * @param expr expression to be converted
   * @return converted expression
   */
  public static LogicalExpression toDrill(DrillParseContext context, RelNode input, RexNode expr) {
    return toDrill(context, Lists.newArrayList(input), expr);
  }

  /**
   * Converts a tree of {@link RexNode} operators into a scalar expression in Drill syntax using multiple inputs.
   *
   * @param context parse context which contains planner settings
   * @param inputs multiple data inputs
   * @param expr expression to be converted
   * @return converted expression
   */
  public static LogicalExpression toDrill(DrillParseContext context, List<RelNode> inputs, RexNode expr) {
    final RexToDrill visitor = new RexToDrill(context, inputs);
    return expr.accept(visitor);
  }

  private static class RexToDrill extends RexVisitorImpl<LogicalExpression> {
    private final List<RelNode> inputs;
    private final DrillParseContext context;
    private final List<RelDataTypeField> fieldList;

    RexToDrill(DrillParseContext context, List<RelNode> inputs) {
      super(true);
      this.context = context;
      this.inputs = inputs;
      this.fieldList = Lists.newArrayList();
      /*
         Fields are enumerated by their presence order in input. Details {@link org.apache.calcite.rex.RexInputRef}.
         Thus we can merge field list from several inputs by adding them into the list in order of appearance.
         Each field index in the list will match field index in the RexInputRef instance which will allow us
         to retrieve field from filed list by index in {@link #visitInputRef(RexInputRef)} method. Example:

         Query: select t1.c1, t2.c1. t2.c2 from t1 inner join t2 on t1.c1 between t2.c1 and t2.c2

         Input 1: $0
         Input 2: $1, $2

         Result: $0, $1, $2
       */
      for (RelNode input : inputs) {
        if (input != null) {
          fieldList.addAll(input.getRowType().getFieldList());
        }
      }
    }

    @Override
    public LogicalExpression visitInputRef(RexInputRef inputRef) {
      final int index = inputRef.getIndex();
      final RelDataTypeField field = fieldList.get(index);
      Preconditions.checkNotNull(field, "Unable to find field using input reference");
      return FieldReference.getWithQuotedRef(field.getName());
    }

    @Override
    public LogicalExpression visitCall(RexCall call) {
//      logger.debug("RexCall {}, {}", call);
      final SqlSyntax syntax = call.getOperator().getSyntax();
      switch (syntax) {
      case BINARY:
        logger.debug("Binary");
        final String funcName = call.getOperator().getName().toLowerCase();
        return doFunction(call, funcName);
      case FUNCTION:
      case FUNCTION_ID:
        logger.debug("Function");
        return getDrillFunctionFromOptiqCall(call);
      case POSTFIX:
        logger.debug("Postfix");
        switch(call.getKind()){
        case IS_NOT_NULL:
        case IS_NOT_TRUE:
        case IS_NOT_FALSE:
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
          case MINUS_PREFIX:
            List<LogicalExpression> operands = Lists.newArrayList();
            operands.add(call.getOperands().get(0).accept(this));
            return FunctionCallFactory.createExpression("u-", operands);
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
              .setIfCondition(new IfCondition(caseArgs.get(i + 1), caseArgs.get(i))).build();
          }
          return elseExpression;
        }

        if (call.getOperator() == SqlStdOperatorTable.ITEM) {
          SchemaPath left = (SchemaPath) call.getOperands().get(0).accept(this);

          // Convert expr of item[*, 'abc'] into column expression 'abc'
          String rootSegName = left.getRootSegment().getPath();
          if (StarColumnHelper.isStarColumn(rootSegName)) {
            rootSegName = rootSegName.substring(0, rootSegName.indexOf(SchemaPath.DYNAMIC_STAR));
            final RexLiteral literal = (RexLiteral) call.getOperands().get(1);
            return SchemaPath.getSimplePath(rootSegName + literal.getValue2().toString());
          }

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

        if (call.getOperator() == SqlStdOperatorTable.DATETIME_PLUS) {
          return doFunction(call, "+");
        }

        if (call.getOperator() == SqlStdOperatorTable.MINUS_DATE) {
          return doFunction(call, "-");
        }

        // fall through
      default:
        throw new AssertionError("todo: implement syntax " + syntax + "(" + call + ")");
      }
    }

    private LogicalExpression doFunction(RexCall call, String funcName) {
      List<LogicalExpression> args = Lists.newArrayList();
      for(RexNode r : call.getOperands()){
        args.add(r.accept(this));
      }

      if (FunctionCallFactory.isBooleanOperator(funcName)) {
        LogicalExpression func = FunctionCallFactory.createBooleanOperator(funcName, args);
        return func;
      } else {
        args = Lists.reverse(args);
        LogicalExpression lastArg = args.get(0);
        for(int i = 1; i < args.size(); i++){
          lastArg = FunctionCallFactory.createExpression(funcName, Lists.newArrayList(args.get(i), lastArg));
        }

        return lastArg;
      }

    }
    private LogicalExpression doUnknown(RexNode o){
      // raise an error
      throw UserException.planError().message(UNSUPPORTED_REX_NODE_ERROR +
              "RexNode Class: %s, RexNode Digest: %s", o.getClass().getName(), o.toString()).build(logger);
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
      MajorType castType;

      switch(call.getType().getSqlTypeName().getName()){
      case "VARCHAR":
      case "CHAR":
        castType = Types.required(MinorType.VARCHAR).toBuilder().setPrecision(call.getType().getPrecision()).build();
        break;

      case "INTEGER": castType = Types.required(MinorType.INT); break;
      case "FLOAT": castType = Types.required(MinorType.FLOAT4); break;
      case "DOUBLE": castType = Types.required(MinorType.FLOAT8); break;
      case "DECIMAL":
        if (!context.getPlannerSettings().getOptions().getOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY).bool_val) {
          throw UserException
              .unsupportedError()
              .message(ExecErrorConstants.DECIMAL_DISABLE_ERR_MSG)
              .build(logger);
        }

        int precision = call.getType().getPrecision();
        int scale = call.getType().getScale();

        if (precision <= 9) {
          castType = TypeProtos.MajorType.newBuilder().setMinorType(MinorType.DECIMAL9).setPrecision(precision).setScale(scale).build();
        } else if (precision <= 18) {
          castType = TypeProtos.MajorType.newBuilder().setMinorType(MinorType.DECIMAL18).setPrecision(precision).setScale(scale).build();
        } else if (precision <= 28) {
          // Inject a cast to SPARSE before casting to the dense type.
          castType = TypeProtos.MajorType.newBuilder().setMinorType(MinorType.DECIMAL28SPARSE).setPrecision(precision).setScale(scale).build();
        } else if (precision <= 38) {
          castType = TypeProtos.MajorType.newBuilder().setMinorType(MinorType.DECIMAL38SPARSE).setPrecision(precision).setScale(scale).build();
        } else {
          throw new UnsupportedOperationException("Only Decimal types with precision range 0 - 38 is supported");
        }
        break;

        case "INTERVAL_YEAR":
        case "INTERVAL_YEAR_MONTH":
        case "INTERVAL_MONTH": castType = Types.required(MinorType.INTERVALYEAR); break;
        case "INTERVAL_DAY":
        case "INTERVAL_DAY_HOUR":
        case "INTERVAL_DAY_MINUTE":
        case "INTERVAL_DAY_SECOND":
        case "INTERVAL_HOUR":
        case "INTERVAL_HOUR_MINUTE":
        case "INTERVAL_HOUR_SECOND":
        case "INTERVAL_MINUTE":
        case "INTERVAL_MINUTE_SECOND":
        case "INTERVAL_SECOND": castType = Types.required(MinorType.INTERVALDAY); break;
        case "BOOLEAN": castType = Types.required(MinorType.BIT); break;
        case "BINARY": castType = Types.required(MinorType.VARBINARY); break;
        case "ANY": return arg; // Type will be same as argument.
        default: castType = Types.required(MinorType.valueOf(call.getType().getSqlTypeName().getName()));
      }
      return FunctionCallFactory.createCast(castType, ExpressionPosition.UNKNOWN, arg);
    }

    private LogicalExpression getDrillFunctionFromOptiqCall(RexCall call) {
      List<LogicalExpression> args = Lists.newArrayList();

      for(RexNode n : call.getOperands()){
        args.add(n.accept(this));
      }

      int argsSize = args.size();
      String functionName = call.getOperator().getName().toLowerCase();

      // TODO: once we have more function rewrites and a patter emerges from different rewrites, factor this out in a better fashion
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
      } else if (functionName.equals("trim")) {
        String trimFunc = null;
        List<LogicalExpression> trimArgs = Lists.newArrayList();

        assert args.get(0) instanceof ValueExpressions.QuotedString;
        switch (((ValueExpressions.QuotedString)args.get(0)).value.toUpperCase()) {
        case "LEADING":
          trimFunc = "ltrim";
          break;
        case "TRAILING":
          trimFunc = "rtrim";
          break;
        case "BOTH":
          trimFunc = "btrim";
          break;
        default:
          assert 1 == 0;
        }

        trimArgs.add(args.get(2));
        trimArgs.add(args.get(1));

        return FunctionCallFactory.createExpression(trimFunc, trimArgs);
      } else if (functionName.equals("date_part")) {
        // Rewrite DATE_PART functions as extract functions
        // assert that the function has exactly two arguments
        assert argsSize == 2;

        /* Based on the first input to the date_part function we rewrite the function as the
         * appropriate extract function. For example
         * date_part('year', date '2008-2-23') ------> extractYear(date '2008-2-23')
         */
        assert args.get(0) instanceof QuotedString;

        QuotedString extractString = (QuotedString) args.get(0);
        String functionPostfix = extractString.value.substring(0, 1).toUpperCase() + extractString.value.substring(1).toLowerCase();
        return FunctionCallFactory.createExpression("extract" + functionPostfix, args.subList(1, 2));
      } else if (functionName.equals("concat")) {

        if (argsSize == 1) {
          /*
           * We treat concat with one argument as a special case. Since we don't have a function
           * implementation of concat that accepts one argument. We simply add another dummy argument
           * (empty string literal) to the list of arguments.
           */
          List<LogicalExpression> concatArgs = new LinkedList<>(args);
          concatArgs.add(QuotedString.EMPTY_STRING);

          return FunctionCallFactory.createExpression(functionName, concatArgs);

        } else if (argsSize > 2) {
          List<LogicalExpression> concatArgs = Lists.newArrayList();

          /* stack concat functions on top of each other if we have more than two arguments
           * Eg: concat(col1, col2, col3) => concat(concat(col1, col2), col3)
           */
          concatArgs.add(args.get(0));
          concatArgs.add(args.get(1));

          LogicalExpression first = FunctionCallFactory.createExpression(functionName, concatArgs);

          for (int i = 2; i < argsSize; i++) {
            concatArgs = Lists.newArrayList();
            concatArgs.add(first);
            concatArgs.add(args.get(i));
            first = FunctionCallFactory.createExpression(functionName, concatArgs);
          }

          return first;
        }
      } else if (functionName.equals("length")) {

          if (argsSize == 2) {

              // Second argument should always be a literal specifying the encoding format
              assert args.get(1) instanceof ValueExpressions.QuotedString;

              String encodingType = ((ValueExpressions.QuotedString) args.get(1)).value;
              functionName += encodingType.substring(0, 1).toUpperCase() + encodingType.substring(1).toLowerCase();

              return FunctionCallFactory.createExpression(functionName, args.subList(0, 1));
          }
      } else if ((functionName.equals("convert_from") || functionName.equals("convert_to"))
                    && args.get(1) instanceof QuotedString) {
        return FunctionCallFactory.createConvert(functionName, ((QuotedString)args.get(1)).value, args.get(0), ExpressionPosition.UNKNOWN);
      } else if (functionName.equals("date_trunc")) {
        return handleDateTruncFunction(args);
      }

      return FunctionCallFactory.createExpression(functionName, args);
    }

    private LogicalExpression handleDateTruncFunction(final List<LogicalExpression> args) {
      // Assert that the first argument to extract is a QuotedString
      assert args.get(0) instanceof ValueExpressions.QuotedString;

      // Get the unit of time to be extracted
      String timeUnitStr = ((ValueExpressions.QuotedString)args.get(0)).value.toUpperCase();

      switch (timeUnitStr){
        case ("YEAR"):
        case ("MONTH"):
        case ("DAY"):
        case ("HOUR"):
        case ("MINUTE"):
        case ("SECOND"):
        case ("WEEK"):
        case ("QUARTER"):
        case ("DECADE"):
        case ("CENTURY"):
        case ("MILLENNIUM"):
          final String functionPostfix = timeUnitStr.substring(0, 1).toUpperCase() + timeUnitStr.substring(1).toLowerCase();
          return FunctionCallFactory.createExpression("date_trunc_" + functionPostfix, args.subList(1, 2));
      }

      throw new UnsupportedOperationException("date_trunc function supports the following time units: " +
          "YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, WEEK, QUARTER, DECADE, CENTURY, MILLENNIUM");
    }


    @Override
    public LogicalExpression visitLiteral(RexLiteral literal) {
      switch(literal.getType().getSqlTypeName()){
      case BIGINT:
        if (isLiteralNull(literal)) {
          return createNullExpr(MinorType.BIGINT);
        }
        long l = (((BigDecimal) literal.getValue()).setScale(0, BigDecimal.ROUND_HALF_UP)).longValue();
        return ValueExpressions.getBigInt(l);
      case BOOLEAN:
        if (isLiteralNull(literal)) {
          return createNullExpr(MinorType.BIT);
        }
        return ValueExpressions.getBit(((Boolean) literal.getValue()));
      case CHAR:
        if (isLiteralNull(literal)) {
          return createStringNullExpr(literal.getType().getPrecision());
        }
        return ValueExpressions.getChar(((NlsString)literal.getValue()).getValue(), literal.getType().getPrecision());
      case DOUBLE:
        if (isLiteralNull(literal)){
          return createNullExpr(MinorType.FLOAT8);
        }
        double d = ((BigDecimal) literal.getValue()).doubleValue();
        return ValueExpressions.getFloat8(d);
      case FLOAT:
        if (isLiteralNull(literal)) {
          return createNullExpr(MinorType.FLOAT4);
        }
        float f = ((BigDecimal) literal.getValue()).floatValue();
        return ValueExpressions.getFloat4(f);
      case INTEGER:
        if (isLiteralNull(literal)) {
          return createNullExpr(MinorType.INT);
        }
        int a = (((BigDecimal) literal.getValue()).setScale(0, BigDecimal.ROUND_HALF_UP)).intValue();
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
        if (isLiteralNull(literal)) {
          return createNullExpr(MinorType.FLOAT8);
        }
        double dbl = ((BigDecimal) literal.getValue()).doubleValue();
        logger.warn("Converting exact decimal into approximate decimal.  Should be fixed once decimal is implemented.");
        return ValueExpressions.getFloat8(dbl);
      case VARCHAR:
        if (isLiteralNull(literal)) {
          return createStringNullExpr(literal.getType().getPrecision());
        }
        return ValueExpressions.getChar(((NlsString)literal.getValue()).getValue(), literal.getType().getPrecision());
      case SYMBOL:
        if (isLiteralNull(literal)) {
          return createStringNullExpr(literal.getType().getPrecision());
        }
        return ValueExpressions.getChar(literal.getValue().toString(), literal.getType().getPrecision());
      case DATE:
        if (isLiteralNull(literal)) {
          return createNullExpr(MinorType.DATE);
        }
        return (ValueExpressions.getDate((GregorianCalendar)literal.getValue()));
      case TIME:
        if (isLiteralNull(literal)) {
          return createNullExpr(MinorType.TIME);
        }
        return (ValueExpressions.getTime((GregorianCalendar)literal.getValue()));
      case TIMESTAMP:
        if (isLiteralNull(literal)) {
          return createNullExpr(MinorType.TIMESTAMP);
        }
        return (ValueExpressions.getTimeStamp((GregorianCalendar) literal.getValue()));
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_YEAR:
      case INTERVAL_MONTH:
        if (isLiteralNull(literal)) {
          return createNullExpr(MinorType.INTERVALYEAR);
        }
        return (ValueExpressions.getIntervalYear(((BigDecimal) (literal.getValue())).intValue()));
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        if (isLiteralNull(literal)) {
          return createNullExpr(MinorType.INTERVALDAY);
        }
        return (ValueExpressions.getIntervalDay(((BigDecimal) (literal.getValue())).longValue()));
      case NULL:
        return NullExpression.INSTANCE;
      case ANY:
        if (isLiteralNull(literal)) {
          return NullExpression.INSTANCE;
        }
      default:
        throw new UnsupportedOperationException(String.format("Unable to convert the value of %s and type %s to a Drill constant expression.", literal, literal.getType().getSqlTypeName()));
      }
    }

    /**
     * Create nullable major type using given minor type
     * and wraps it in typed null constant.
     *
     * @param type minor type
     * @return typed null constant instance
     */
    private TypedNullConstant createNullExpr(MinorType type) {
      return new TypedNullConstant(Types.optional(type));
    }

    /**
     * Create nullable varchar major type with given precision
     * and wraps it in typed null constant.
     *
     * @param precision precision value
     * @return typed null constant instance
     */
    private TypedNullConstant createStringNullExpr(int precision) {
      return new TypedNullConstant(Types.withPrecision(MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL, precision));
    }
  }

  public static boolean isLiteralNull(RexLiteral literal) {
    return literal.getTypeName().getName().equals("NULL");
  }
}
