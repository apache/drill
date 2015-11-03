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
package org.apache.drill.exec.expr;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.ConvertExpression;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.NullExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.TypedNullConstant;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DateExpression;
import org.apache.drill.common.expression.ValueExpressions.Decimal18Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal28Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal38Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal9Expression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.IntervalDayExpression;
import org.apache.drill.common.expression.ValueExpressions.IntervalYearExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.ValueExpressions.TimeExpression;
import org.apache.drill.common.expression.ValueExpressions.TimeStampExpression;
import org.apache.drill.common.expression.fn.CastFunctions;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.expression.visitors.ConditionalExprOptimizer;
import org.apache.drill.common.expression.visitors.ExpressionValidator;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.util.CoreDecimalUtility;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.fn.AbstractFuncHolder;
import org.apache.drill.exec.expr.fn.DrillComplexWriterFuncHolder;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.ExceptionFunction;
import org.apache.drill.exec.expr.fn.FunctionLookupContext;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.resolver.FunctionResolver;
import org.apache.drill.exec.resolver.FunctionResolverFactory;
import org.apache.drill.exec.resolver.TypeCastRules;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class ExpressionTreeMaterializer {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionTreeMaterializer.class);

  private ExpressionTreeMaterializer() {
  };

  public static LogicalExpression materialize(LogicalExpression expr, VectorAccessible batch, ErrorCollector errorCollector, FunctionLookupContext functionLookupContext) {
    return ExpressionTreeMaterializer.materialize(expr, batch, errorCollector, functionLookupContext, false, false);
  }

  public static LogicalExpression materializeAndCheckErrors(LogicalExpression expr, VectorAccessible batch, FunctionLookupContext functionLookupContext) throws SchemaChangeException {
    ErrorCollector collector = new ErrorCollectorImpl();
    LogicalExpression e = ExpressionTreeMaterializer.materialize(expr, batch, collector, functionLookupContext, false, false);
    if (collector.hasErrors()) {
      throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
    }
    return e;
  }

  public static LogicalExpression materialize(LogicalExpression expr, VectorAccessible batch, ErrorCollector errorCollector, FunctionLookupContext functionLookupContext,
                                              boolean allowComplexWriterExpr) {
    return materialize(expr, batch, errorCollector, functionLookupContext, allowComplexWriterExpr, false);
  }

  public static LogicalExpression materialize(LogicalExpression expr, VectorAccessible batch, ErrorCollector errorCollector, FunctionLookupContext functionLookupContext,
      boolean allowComplexWriterExpr, boolean unionTypeEnabled) {
    LogicalExpression out =  expr.accept(new MaterializeVisitor(batch, errorCollector, allowComplexWriterExpr, unionTypeEnabled), functionLookupContext);

    if (!errorCollector.hasErrors()) {
      out = out.accept(ConditionalExprOptimizer.INSTANCE, null);
    }

    if (out instanceof NullExpression) {
      return new TypedNullConstant(Types.optional(MinorType.INT));
    } else {
      return out;
    }
  }

  public static LogicalExpression convertToNullableType(LogicalExpression fromExpr, MinorType toType, FunctionLookupContext functionLookupContext, ErrorCollector errorCollector) {
    String funcName = "convertToNullable" + toType.toString();
    List<LogicalExpression> args = Lists.newArrayList();
    args.add(fromExpr);
    FunctionCall funcCall = new FunctionCall(funcName, args, ExpressionPosition.UNKNOWN);
    FunctionResolver resolver = FunctionResolverFactory.getResolver(funcCall);

    DrillFuncHolder matchedConvertToNullableFuncHolder = functionLookupContext.findDrillFunction(resolver, funcCall);
    if (matchedConvertToNullableFuncHolder == null) {
      logFunctionResolutionError(errorCollector, funcCall);
      return NullExpression.INSTANCE;
    }

    return matchedConvertToNullableFuncHolder.getExpr(funcName, args, ExpressionPosition.UNKNOWN);
  }


  public static LogicalExpression addCastExpression(LogicalExpression fromExpr, MajorType toType, FunctionLookupContext functionLookupContext, ErrorCollector errorCollector) {
    String castFuncName = CastFunctions.getCastFunc(toType.getMinorType());
    List<LogicalExpression> castArgs = Lists.newArrayList();
    castArgs.add(fromExpr);  //input_expr

    if (fromExpr.getMajorType().getMinorType() == MinorType.UNION && toType.getMinorType() == MinorType.UNION) {
      return fromExpr;
    }

    if (!Types.isFixedWidthType(toType) && !Types.isUnion(toType)) {

      /* We are implicitly casting to VARCHAR so we don't have a max length,
       * using an arbitrary value. We trim down the size of the stored bytes
       * to the actual size so this size doesn't really matter.
       */
      castArgs.add(new ValueExpressions.LongExpression(TypeHelper.VARCHAR_DEFAULT_CAST_LEN, null));
    }
    else if (CoreDecimalUtility.isDecimalType(toType)) {
      // Add the scale and precision to the arguments of the implicit cast
      castArgs.add(new ValueExpressions.LongExpression(toType.getPrecision(), null));
      castArgs.add(new ValueExpressions.LongExpression(toType.getScale(), null));
    }
    FunctionCall castCall = new FunctionCall(castFuncName, castArgs, ExpressionPosition.UNKNOWN);
    FunctionResolver resolver = FunctionResolverFactory.getExactResolver(castCall);
    DrillFuncHolder matchedCastFuncHolder = functionLookupContext.findDrillFunction(resolver, castCall);

    if (matchedCastFuncHolder == null) {
      logFunctionResolutionError(errorCollector, castCall);
      return NullExpression.INSTANCE;
    }
    return matchedCastFuncHolder.getExpr(castFuncName, castArgs, ExpressionPosition.UNKNOWN);
  }

  private static void logFunctionResolutionError(ErrorCollector errorCollector, FunctionCall call) {
    // add error to collector
    StringBuilder sb = new StringBuilder();
    sb.append("Missing function implementation: ");
    sb.append("[");
    sb.append(call.getName());
    sb.append("(");
    boolean first = true;
    for(LogicalExpression e : call.args) {
      TypeProtos.MajorType mt = e.getMajorType();
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      sb.append(mt.getMinorType().name());
      sb.append("-");
      sb.append(mt.getMode().name());
    }
    sb.append(")");
    sb.append("]");

    errorCollector.addGeneralError(call.getPosition(), sb.toString());
  }

  private static class MaterializeVisitor extends AbstractExprVisitor<LogicalExpression, FunctionLookupContext, RuntimeException> {
    private ExpressionValidator validator = new ExpressionValidator();
    private ErrorCollector errorCollector;
    private Deque<ErrorCollector> errorCollectors = new ArrayDeque<>();
    private final VectorAccessible batch;
    private final boolean allowComplexWriter;
    /**
     * If this is false, the materializer will not handle or create UnionTypes
     * Once this code is more well tested, we will probably remove this flag
     */
    private final boolean unionTypeEnabled;

    public MaterializeVisitor(VectorAccessible batch, ErrorCollector errorCollector, boolean allowComplexWriter, boolean unionTypeEnabled) {
      this.batch = batch;
      this.errorCollector = errorCollector;
      this.allowComplexWriter = allowComplexWriter;
      this.unionTypeEnabled = unionTypeEnabled;
    }

    private LogicalExpression validateNewExpr(LogicalExpression newExpr) {
      newExpr.accept(validator, errorCollector);
      return newExpr;
    }

    @Override
    public LogicalExpression visitUnknown(LogicalExpression e, FunctionLookupContext functionLookupContext)
      throws RuntimeException {
      return e;
    }

    @Override
    public LogicalExpression visitFunctionHolderExpression(FunctionHolderExpression holder, FunctionLookupContext functionLookupContext) throws RuntimeException {
      // a function holder is already materialized, no need to rematerialize.  generally this won't be used unless we materialize a partial tree and rematerialize the whole tree.
      return holder;
    }

    @Override
    public LogicalExpression visitBooleanOperator(BooleanOperator op, FunctionLookupContext functionLookupContext) {
      List<LogicalExpression> args = Lists.newArrayList();
      for (int i = 0; i < op.args.size(); ++i) {
        LogicalExpression newExpr = op.args.get(i).accept(this, functionLookupContext);
        assert newExpr != null : String.format("Materialization of %s return a null expression.", op.args.get(i));
        args.add(newExpr);
      }

      //replace with a new function call, since its argument could be changed.
      return new BooleanOperator(op.getName(), args, op.getPosition());
    }

    @Override
    public LogicalExpression visitFunctionCall(FunctionCall call, FunctionLookupContext functionLookupContext) {
      List<LogicalExpression> args = Lists.newArrayList();
      for (int i = 0; i < call.args.size(); ++i) {
        LogicalExpression newExpr = call.args.get(i).accept(this, functionLookupContext);
        assert newExpr != null : String.format("Materialization of %s returned a null expression.", call.args.get(i));
        args.add(newExpr);
      }

      //replace with a new function call, since its argument could be changed.
      call = new FunctionCall(call.getName(), args, call.getPosition());

      FunctionResolver resolver = FunctionResolverFactory.getResolver(call);
      DrillFuncHolder matchedFuncHolder = functionLookupContext.findDrillFunction(resolver, call);

      if (matchedFuncHolder instanceof DrillComplexWriterFuncHolder && ! allowComplexWriter) {
        errorCollector.addGeneralError(call.getPosition(), "Only ProjectRecordBatch could have complex writer function. You are using complex writer function " + call.getName() + " in a non-project operation!");
      }

      //new arg lists, possible with implicit cast inserted.
      List<LogicalExpression> argsWithCast = Lists.newArrayList();

      if (matchedFuncHolder!=null) {
        //Compare parm type against arg type. Insert cast on top of arg, whenever necessary.
        for (int i = 0; i < call.args.size(); ++i) {

          LogicalExpression currentArg = call.args.get(i);

          TypeProtos.MajorType parmType = matchedFuncHolder.getParmMajorType(i);

          //Case 1: If  1) the argument is NullExpression
          //            2) the parameter of matchedFuncHolder allows null input, or func's null_handling is NULL_IF_NULL (means null and non-null are exchangable).
          //        then replace NullExpression with a TypedNullConstant
          if (currentArg.equals(NullExpression.INSTANCE) &&
            ( parmType.getMode().equals(TypeProtos.DataMode.OPTIONAL) ||
              matchedFuncHolder.getNullHandling() == FunctionTemplate.NullHandling.NULL_IF_NULL)) {
            argsWithCast.add(new TypedNullConstant(parmType));
          } else if (Types.softEquals(parmType, currentArg.getMajorType(), matchedFuncHolder.getNullHandling() == FunctionTemplate.NullHandling.NULL_IF_NULL) ||
                     matchedFuncHolder.isFieldReader(i)) {
            //Case 2: argument and parameter matches, or parameter is FieldReader.  Do nothing.
            argsWithCast.add(currentArg);
          } else {
            //Case 3: insert cast if param type is different from arg type.
            if (CoreDecimalUtility.isDecimalType(parmType)) {
              // We are implicitly promoting a decimal type, set the required scale and precision
              parmType = MajorType.newBuilder().setMinorType(parmType.getMinorType()).setMode(parmType.getMode()).
                  setScale(currentArg.getMajorType().getScale()).setPrecision(currentArg.getMajorType().getPrecision()).build();
            }
            argsWithCast.add(addCastExpression(currentArg, parmType, functionLookupContext, errorCollector));
          }
        }

        return matchedFuncHolder.getExpr(call.getName(), argsWithCast, call.getPosition());
      }

      // as no drill func is found, search for a non-Drill function.
      AbstractFuncHolder matchedNonDrillFuncHolder = functionLookupContext.findNonDrillFunction(call);
      if (matchedNonDrillFuncHolder != null) {
        // Insert implicit cast function holder expressions if required
        List<LogicalExpression> extArgsWithCast = Lists.newArrayList();

        for (int i = 0; i < call.args.size(); ++i) {
          LogicalExpression currentArg = call.args.get(i);
          TypeProtos.MajorType parmType = matchedNonDrillFuncHolder.getParmMajorType(i);

          if (Types.softEquals(parmType, currentArg.getMajorType(), true)) {
            extArgsWithCast.add(currentArg);
          } else {
            // Insert cast if param type is different from arg type.
            if (CoreDecimalUtility.isDecimalType(parmType)) {
              // We are implicitly promoting a decimal type, set the required scale and precision
              parmType = MajorType.newBuilder().setMinorType(parmType.getMinorType()).setMode(parmType.getMode()).
                  setScale(currentArg.getMajorType().getScale()).setPrecision(currentArg.getMajorType().getPrecision()).build();
            }
            extArgsWithCast.add(addCastExpression(call.args.get(i), parmType, functionLookupContext, errorCollector));
          }
        }

        return matchedNonDrillFuncHolder.getExpr(call.getName(), extArgsWithCast, call.getPosition());
      }

      if (hasUnionInput(call)) {
        return rewriteUnionFunction(call, functionLookupContext);
      }

      logFunctionResolutionError(errorCollector, call);
      return NullExpression.INSTANCE;
    }

    private static final Set<String> UNION_FUNCTIONS;

    static {
      UNION_FUNCTIONS = new HashSet<>();
      for (MinorType t : MinorType.values()) {
        UNION_FUNCTIONS.add("assert_" + t.name().toLowerCase());
        UNION_FUNCTIONS.add("is_" + t.name().toLowerCase());
      }
      UNION_FUNCTIONS.add("typeof");
    }

    private boolean hasUnionInput(FunctionCall call) {
      for (LogicalExpression arg : call.args) {
        if (arg.getMajorType().getMinorType() == MinorType.UNION) {
          return true;
        }
      }
      return false;
    }

    /**
     * Converts a function call with a Union type input into a case statement, where each branch of the case corresponds to
     * one of the subtypes of the Union type. The function call is materialized in each of the branches, with the union input cast
     * to the specific type corresponding to the branch of the case statement
     * @param call
     * @param functionLookupContext
     * @return
     */
    private LogicalExpression rewriteUnionFunction(FunctionCall call, FunctionLookupContext functionLookupContext) {
      LogicalExpression[] args = new LogicalExpression[call.args.size()];
      call.args.toArray(args);

      for (int i = 0; i < args.length; i++) {
        LogicalExpression arg = call.args.get(i);
        MajorType majorType = arg.getMajorType();

        if (majorType.getMinorType() != MinorType.UNION) {
          continue;
        }

        List<MinorType> subTypes = majorType.getSubTypeList();
        Preconditions.checkState(subTypes.size() > 0, "Union type has no subtypes");

        Queue<IfCondition> ifConditions = Lists.newLinkedList();

        for (MinorType minorType : subTypes) {
          LogicalExpression ifCondition = getIsTypeExpressionForType(minorType, arg.accept(new CloneVisitor(), null));
          args[i] = getUnionAssertFunctionForType(minorType, arg.accept(new CloneVisitor(), null));

          List<LogicalExpression> newArgs = Lists.newArrayList();
          for (LogicalExpression e : args) {
            newArgs.add(e.accept(new CloneVisitor(), null));
          }

          // When expanding the expression tree to handle the different subtypes, we will not throw an exception if one
          // of the branches fails to find a function match, since it is possible that code path will never occur in execution
          // So instead of failing to materialize, we generate code to throw the exception during execution if that code
          // path is hit.

          errorCollectors.push(errorCollector);
          errorCollector = new ErrorCollectorImpl();

          LogicalExpression thenExpression = new FunctionCall(call.getName(), newArgs, call.getPosition()).accept(this, functionLookupContext);

          if (errorCollector.hasErrors()) {
            thenExpression = getExceptionFunction(errorCollector.toErrorString());
          }

          errorCollector = errorCollectors.pop();

          IfExpression.IfCondition condition = new IfCondition(ifCondition, thenExpression);
          ifConditions.add(condition);
        }

        LogicalExpression ifExpression = ifConditions.poll().expression;

        while (!ifConditions.isEmpty()) {
          ifExpression = IfExpression.newBuilder().setIfCondition(ifConditions.poll()).setElse(ifExpression).build();
        }

        args[i] = ifExpression;
        return ifExpression.accept(this, functionLookupContext);
      }
      throw new UnsupportedOperationException("Did not find any Union input types");
    }

    /**
     * Returns the function call whose purpose is to throw an Exception if that code is hit during execution
     * @param message the exception message
     * @return
     */
    private LogicalExpression getExceptionFunction(String message) {
      QuotedString msg = new QuotedString(message, ExpressionPosition.UNKNOWN);
      List<LogicalExpression> args = Lists.newArrayList();
      args.add(msg);
      FunctionCall call = new FunctionCall(ExceptionFunction.EXCEPTION_FUNCTION_NAME, args, ExpressionPosition.UNKNOWN);
      return call;
    }

    /**
     * Returns the function which asserts that the current subtype of a union type is a specific type, and allows the materializer
     * to bind to that specific type when doing function resolution
     * @param type
     * @param arg
     * @return
     */
    private LogicalExpression getUnionAssertFunctionForType(MinorType type, LogicalExpression arg) {
      if (type == MinorType.UNION) {
        return arg;
      }
      if (type == MinorType.LIST || type == MinorType.MAP) {
        return getExceptionFunction("Unable to cast union to " + type);
      }
      String castFuncName = String.format("assert_%s", type.toString());
      Collections.singletonList(arg);
      return new FunctionCall(castFuncName, Collections.singletonList(arg), ExpressionPosition.UNKNOWN);
    }

    /**
     * Get the function that tests whether a union type is a specific type
     * @param type
     * @param arg
     * @return
     */
    private LogicalExpression getIsTypeExpressionForType(MinorType type, LogicalExpression arg) {
      String isFuncName = String.format("is_%s", type.toString());
      List<LogicalExpression> args = Lists.newArrayList();
      args.add(arg);
      return new FunctionCall(isFuncName, args, ExpressionPosition.UNKNOWN);
    }

    @Override
    public LogicalExpression visitIfExpression(IfExpression ifExpr, FunctionLookupContext functionLookupContext) {
      IfExpression.IfCondition conditions = ifExpr.ifCondition;
      LogicalExpression newElseExpr = ifExpr.elseExpression.accept(this, functionLookupContext);

      LogicalExpression newCondition = conditions.condition.accept(this, functionLookupContext);
      LogicalExpression newExpr = conditions.expression.accept(this, functionLookupContext);
      conditions = new IfExpression.IfCondition(newCondition, newExpr);

      MinorType thenType = conditions.expression.getMajorType().getMinorType();
      MinorType elseType = newElseExpr.getMajorType().getMinorType();
      boolean hasUnion = thenType == MinorType.UNION || elseType == MinorType.UNION;
      if (unionTypeEnabled) {
        if (thenType != elseType && !(thenType == MinorType.NULL || elseType == MinorType.NULL)) {

          MinorType leastRestrictive = MinorType.UNION;
          MajorType.Builder builder = MajorType.newBuilder().setMinorType(MinorType.UNION).setMode(DataMode.OPTIONAL);
          if (thenType == MinorType.UNION) {
            for (MinorType subType : conditions.expression.getMajorType().getSubTypeList()) {
              builder.addSubType(subType);
            }
          } else {
            builder.addSubType(thenType);
          }
          if (elseType == MinorType.UNION) {
            for (MinorType subType : newElseExpr.getMajorType().getSubTypeList()) {
              builder.addSubType(subType);
            }
          } else {
            builder.addSubType(elseType);
          }
          MajorType unionType = builder.build();
          conditions = new IfExpression.IfCondition(newCondition,
                  addCastExpression(conditions.expression, unionType, functionLookupContext, errorCollector));
          newElseExpr = addCastExpression(newElseExpr, unionType, functionLookupContext, errorCollector);
        }

      } else {
        // Check if we need a cast
        if (thenType != elseType && !(thenType == MinorType.NULL || elseType == MinorType.NULL)) {

          MinorType leastRestrictive = TypeCastRules.getLeastRestrictiveType((Arrays.asList(thenType, elseType)));
          if (leastRestrictive != thenType) {
            // Implicitly cast the then expression
            conditions = new IfExpression.IfCondition(newCondition,
            addCastExpression(conditions.expression, newElseExpr.getMajorType(), functionLookupContext, errorCollector));
          } else if (leastRestrictive != elseType) {
            // Implicitly cast the else expression
            newElseExpr = addCastExpression(newElseExpr, conditions.expression.getMajorType(), functionLookupContext, errorCollector);
          } else {
            /* Cannot cast one of the two expressions to make the output type of if and else expression
             * to be the same. Raise error.
             */
            throw new DrillRuntimeException("Case expression should have similar output type on all its branches");
          }
        }
      }

      // Resolve NullExpression into TypedNullConstant by visiting all conditions
      // We need to do this because we want to give the correct MajorType to the Null constant
      List<LogicalExpression> allExpressions = Lists.newArrayList();
      allExpressions.add(conditions.expression);
      allExpressions.add(newElseExpr);

      boolean containsNullExpr = Iterables.any(allExpressions, new Predicate<LogicalExpression>() {
        @Override
        public boolean apply(LogicalExpression input) {
          return input instanceof NullExpression;
        }
      });

      if (containsNullExpr) {
        Optional<LogicalExpression> nonNullExpr = Iterables.tryFind(allExpressions,
          new Predicate<LogicalExpression>() {
            @Override
            public boolean apply(LogicalExpression input) {
              return !input.getMajorType().getMinorType().equals(TypeProtos.MinorType.NULL);
            }
          }
        );

        if(nonNullExpr.isPresent()) {
          MajorType type = nonNullExpr.get().getMajorType();
          conditions = new IfExpression.IfCondition(conditions.condition, rewriteNullExpression(conditions.expression, type));

          newElseExpr = rewriteNullExpression(newElseExpr, type);
        }
      }

      if (!hasUnion) {
        // If the type of the IF expression is nullable, apply a convertToNullable*Holder function for "THEN"/"ELSE"
        // expressions whose type is not nullable.
        if (IfExpression.newBuilder().setElse(newElseExpr).setIfCondition(conditions).build().getMajorType().getMode()
                == DataMode.OPTIONAL) {
          IfExpression.IfCondition condition = conditions;
          if (condition.expression.getMajorType().getMode() != DataMode.OPTIONAL) {
            conditions = new IfExpression.IfCondition(condition.condition, getConvertToNullableExpr(ImmutableList.of(condition.expression),
                    condition.expression.getMajorType().getMinorType(), functionLookupContext));
          }

          if (newElseExpr.getMajorType().getMode() != DataMode.OPTIONAL) {
            newElseExpr = getConvertToNullableExpr(ImmutableList.of(newElseExpr),
                    newElseExpr.getMajorType().getMinorType(), functionLookupContext);
          }
        }
      }

      return validateNewExpr(IfExpression.newBuilder().setElse(newElseExpr).setIfCondition(conditions).build());
    }

    private LogicalExpression getConvertToNullableExpr(List<LogicalExpression> args, MinorType minorType,
        FunctionLookupContext functionLookupContext) {
      String funcName = "convertToNullable" + minorType.toString();
      FunctionCall funcCall = new FunctionCall(funcName, args, ExpressionPosition.UNKNOWN);
      FunctionResolver resolver = FunctionResolverFactory.getResolver(funcCall);

      DrillFuncHolder matchedConvertToNullableFuncHolder = functionLookupContext.findDrillFunction(resolver, funcCall);

      if (matchedConvertToNullableFuncHolder == null) {
        logFunctionResolutionError(errorCollector, funcCall);
        return NullExpression.INSTANCE;
      }

      return matchedConvertToNullableFuncHolder.getExpr(funcName, args, ExpressionPosition.UNKNOWN);
    }

    private LogicalExpression rewriteNullExpression(LogicalExpression expr, MajorType type) {
      if(expr instanceof NullExpression) {
        return new TypedNullConstant(type);
      } else {
        return expr;
      }
    }

    @Override
    public LogicalExpression visitSchemaPath(SchemaPath path, FunctionLookupContext functionLookupContext) {
//      logger.debug("Visiting schema path {}", path);
      TypedFieldId tfId = batch.getValueVectorId(path);
      if (tfId == null) {
        logger.warn("Unable to find value vector of path {}, returning null instance.", path);
        return NullExpression.INSTANCE;
      } else {
        ValueVectorReadExpression e = new ValueVectorReadExpression(tfId);
        return e;
      }
    }

    @Override
    public LogicalExpression visitIntConstant(IntExpression intExpr, FunctionLookupContext functionLookupContext) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitFloatConstant(FloatExpression fExpr, FunctionLookupContext functionLookupContext) {
      return fExpr;
    }

    @Override
    public LogicalExpression visitLongConstant(LongExpression intExpr, FunctionLookupContext functionLookupContext) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitDateConstant(DateExpression intExpr, FunctionLookupContext functionLookupContext) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitTimeConstant(TimeExpression intExpr, FunctionLookupContext functionLookupContext) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitTimeStampConstant(TimeStampExpression intExpr, FunctionLookupContext functionLookupContext) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitNullConstant(TypedNullConstant nullConstant, FunctionLookupContext functionLookupContext) throws RuntimeException {
      return nullConstant;
    }

    @Override
    public LogicalExpression visitIntervalYearConstant(IntervalYearExpression intExpr, FunctionLookupContext functionLookupContext) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitIntervalDayConstant(IntervalDayExpression intExpr, FunctionLookupContext functionLookupContext) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitDecimal9Constant(Decimal9Expression decExpr, FunctionLookupContext functionLookupContext) {
      return decExpr;
    }

    @Override
    public LogicalExpression visitDecimal18Constant(Decimal18Expression decExpr, FunctionLookupContext functionLookupContext) {
      return decExpr;
    }

    @Override
    public LogicalExpression visitDecimal28Constant(Decimal28Expression decExpr, FunctionLookupContext functionLookupContext) {
      return decExpr;
    }

    @Override
    public LogicalExpression visitDecimal38Constant(Decimal38Expression decExpr, FunctionLookupContext functionLookupContext) {
      return decExpr;
    }

    @Override
    public LogicalExpression visitDoubleConstant(DoubleExpression dExpr, FunctionLookupContext functionLookupContext) {
      return dExpr;
    }

    @Override
    public LogicalExpression visitBooleanConstant(BooleanExpression e, FunctionLookupContext functionLookupContext) {
      return e;
    }

    @Override
    public LogicalExpression visitQuotedStringConstant(QuotedString e, FunctionLookupContext functionLookupContext) {
      return e;
    }

    @Override
    public LogicalExpression visitConvertExpression(ConvertExpression e, FunctionLookupContext functionLookupContext) {
      String convertFunctionName = e.getConvertFunction() + e.getEncodingType();

      List<LogicalExpression> newArgs = Lists.newArrayList();
      newArgs.add(e.getInput());  //input_expr

      FunctionCall fc = new FunctionCall(convertFunctionName, newArgs, e.getPosition());
      return fc.accept(this, functionLookupContext);
    }

    @Override
    public LogicalExpression visitCastExpression(CastExpression e, FunctionLookupContext functionLookupContext) {

      // if the cast is pointless, remove it.
      LogicalExpression input = e.getInput().accept(this,  functionLookupContext);

      MajorType newMajor = e.getMajorType(); // Output type
      MinorType newMinor = input.getMajorType().getMinorType(); // Input type

      if (castEqual(e.getPosition(), input.getMajorType(), newMajor)) {
        return input; // don't do pointless cast.
      }

      if (newMinor == MinorType.LATE) {
        // if the type still isn't fully bound, leave as cast expression.
        return new CastExpression(input, e.getMajorType(), e.getPosition());
      } else if (newMinor == MinorType.NULL) {
        // if input is a NULL expression, remove cast expression and return a TypedNullConstant directly.
        return new TypedNullConstant(Types.optional(e.getMajorType().getMinorType()));
      } else {
        // if the type is fully bound, convert to functioncall and materialze the function.
        MajorType type = e.getMajorType();

        // Get the cast function name from the map
        String castFuncWithType = CastFunctions.getCastFunc(type.getMinorType());

        List<LogicalExpression> newArgs = Lists.newArrayList();
        newArgs.add(e.getInput());  //input_expr

        //VarLen type
        if (!Types.isFixedWidthType(type)) {
          newArgs.add(new ValueExpressions.LongExpression(type.getWidth(), null));
        }  if (CoreDecimalUtility.isDecimalType(type)) {
            newArgs.add(new ValueExpressions.LongExpression(type.getPrecision(), null));
            newArgs.add(new ValueExpressions.LongExpression(type.getScale(), null));
        }
        FunctionCall fc = new FunctionCall(castFuncWithType, newArgs, e.getPosition());
        return fc.accept(this, functionLookupContext);
      }
    }

    private boolean castEqual(ExpressionPosition pos, MajorType from, MajorType to) {
      if (!from.getMinorType().equals(to.getMinorType())) {
        return false;
      }
      switch(from.getMinorType()) {
      case FLOAT4:
      case FLOAT8:
      case INT:
      case BIGINT:
      case BIT:
      case TINYINT:
      case SMALLINT:
      case UINT1:
      case UINT2:
      case UINT4:
      case UINT8:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMPTZ:
      case DATE:
      case INTERVAL:
      case INTERVALDAY:
      case INTERVALYEAR:
        // nothing else matters.
        return true;
      case DECIMAL9:
      case DECIMAL18:
      case DECIMAL28DENSE:
      case DECIMAL28SPARSE:
      case DECIMAL38DENSE:
      case DECIMAL38SPARSE:
        if (to.getScale() == from.getScale() && to.getPrecision() == from.getPrecision()) {
          return true;
        }
        return false;

      case FIXED16CHAR:
      case FIXEDBINARY:
      case FIXEDCHAR:
        // width always matters
        this.errorCollector.addGeneralError(pos, "Casting fixed width types are not yet supported..");
        return false;

      case VAR16CHAR:
      case VARBINARY:
      case VARCHAR:
        // We could avoid redundant cast:
        // 1) when "to" length is no smaller than "from" length and "from" length is known (>0),
        // 2) or "to" length is unknown (0 means unknown length?).
        // Case 1 and case 2 mean that cast will do nothing.
        // In other cases, cast is required to trim the "from" according to "to" length.
        if ( (to.getWidth() >= from.getWidth() && from.getWidth() > 0) || to.getWidth() == 0) {
          return true;
        } else {
          return false;
        }

      default:
        errorCollector.addGeneralError(pos, String.format("Casting rules are unknown for type %s.", from));
        return false;
      }
    }
  }
}
