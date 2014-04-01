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

import java.util.List;

import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.TypedNullConstant;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.DateExpression;
import org.apache.drill.common.expression.ValueExpressions.IntervalYearExpression;
import org.apache.drill.common.expression.ValueExpressions.IntervalDayExpression;
import org.apache.drill.common.expression.ValueExpressions.TimeStampExpression;
import org.apache.drill.common.expression.ValueExpressions.TimeExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.fn.CastFunctions;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.expression.visitors.ExpressionValidator;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.HiveFuncHolder;
import org.apache.drill.exec.record.NullExpression;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.resolver.FunctionResolver;
import org.apache.drill.exec.resolver.FunctionResolverFactory;

import com.google.common.collect.Lists;

public class ExpressionTreeMaterializer {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionTreeMaterializer.class);
  
  private ExpressionTreeMaterializer() {
  };

  public static LogicalExpression materialize(LogicalExpression expr, VectorAccessible batch, ErrorCollector errorCollector, FunctionImplementationRegistry registry) {
    LogicalExpression out =  expr.accept(new MaterializeVisitor(batch, errorCollector), registry);
    if(out instanceof NullExpression){
      return new TypedNullConstant(Types.optional(MinorType.INT));
    }else{
      return out;
    }
  }

  private static class MaterializeVisitor extends AbstractExprVisitor<LogicalExpression, FunctionImplementationRegistry, RuntimeException> {
    private ExpressionValidator validator = new ExpressionValidator();
    private final ErrorCollector errorCollector;
    private final VectorAccessible batch;

    public MaterializeVisitor(VectorAccessible batch, ErrorCollector errorCollector) {
      this.batch = batch;
      this.errorCollector = errorCollector;
    }

    private LogicalExpression validateNewExpr(LogicalExpression newExpr) {
      newExpr.accept(validator, errorCollector);
      return newExpr;
    }

    @Override
    public LogicalExpression visitUnknown(LogicalExpression e, FunctionImplementationRegistry registry)
      throws RuntimeException {
      return e;
    }

    @Override
    public LogicalExpression visitFunctionHolderExpression(FunctionHolderExpression holder, FunctionImplementationRegistry value) throws RuntimeException {
      // a function holder is already materialized, no need to rematerialize.  generally this won't be used unless we materialize a partial tree and rematerialize the whole tree.
      return holder;
    }

    @Override
    public LogicalExpression visitFunctionCall(FunctionCall call, FunctionImplementationRegistry registry) {
      List<LogicalExpression> args = Lists.newArrayList();
      for (int i = 0; i < call.args.size(); ++i) {
        LogicalExpression newExpr = call.args.get(i).accept(this, registry);
        assert newExpr != null : String.format("Materialization of %s return a null expression.", call.args.get(i));
        args.add(newExpr);
      }

      //replace with a new function call, since its argument could be changed.
      call = new FunctionCall(call.getName(), args, call.getPosition());

      // First try to resolve as Drill function, if that fails try resolving as hive function
      // TODO: Need to refactor the resolver to have generic interface for Drill and Hive functions

      FunctionResolver resolver = FunctionResolverFactory.getResolver(call);
      DrillFuncHolder matchedFuncHolder =
        resolver.getBestMatch(registry.getDrillRegistry().getMethods().get(call.getName()), call);

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
          } else if (Types.softEquals(parmType, currentArg.getMajorType(), matchedFuncHolder.getNullHandling() ==
            FunctionTemplate.NullHandling.NULL_IF_NULL)) {
            //Case 2: argument and parameter matches. Do nothing.
            argsWithCast.add(call.args.get(i));
          } else {
            //Case 3: insert cast if param type is different from arg type.

            // Create the desired output type and CasExpression and traverse the visitor pattern
            CastExpression castExpression = new CastExpression(currentArg, parmType, ExpressionPosition.UNKNOWN);
            argsWithCast.add(castExpression.accept(this, registry));
          }
        }
        return new DrillFuncHolderExpr(call.getName(), matchedFuncHolder, argsWithCast, call.getPosition());
      }

      // as no drill func is found, search for the function in hive
      HiveFuncHolder matchedHiveHolder = registry.getHiveRegistry().getFunction(call);
      if (matchedHiveHolder != null)
        return new HiveFuncHolderExpr(call.getName(), matchedHiveHolder, call.args, call.getPosition());

      logFunctionResolutionError(errorCollector, call);
      return null;
    }

    private void logFunctionResolutionError(ErrorCollector errorCollector, FunctionCall call) {
      // add error to collector
      StringBuilder sb = new StringBuilder();
      sb.append("Missing function implementation: ");
      sb.append("[");
      sb.append(call.getName());
      sb.append("(");
      boolean first = true;
      for(LogicalExpression e : call.args) {
        TypeProtos.MajorType mt = e.getMajorType();
        if(first){
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

    @Override
    public LogicalExpression visitIfExpression(IfExpression ifExpr, FunctionImplementationRegistry registry) {
      List<IfExpression.IfCondition> conditions = Lists.newArrayList(ifExpr.conditions);
      LogicalExpression newElseExpr = ifExpr.elseExpression.accept(this, registry);

      for (int i = 0; i < conditions.size(); ++i) {
        IfExpression.IfCondition condition = conditions.get(i);

        LogicalExpression newCondition = condition.condition.accept(this, registry);
        LogicalExpression newExpr = condition.expression.accept(this, registry);
        conditions.set(i, new IfExpression.IfCondition(newCondition, newExpr));
      }

      return validateNewExpr(IfExpression.newBuilder().setElse(newElseExpr).addConditions(conditions).build());
    }

    @Override
    public LogicalExpression visitSchemaPath(SchemaPath path, FunctionImplementationRegistry value) {
//      logger.debug("Visiting schema path {}", path);
      TypedFieldId tfId = batch.getValueVectorId(path);
      if (tfId == null) {
        logger.warn("Unable to find value vector of path {}, returning null instance.", path);
        return NullExpression.INSTANCE;
      } else {
        return new ValueVectorReadExpression(tfId);
      }
    }

    @Override
    public LogicalExpression visitIntConstant(IntExpression intExpr, FunctionImplementationRegistry value) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitFloatConstant(FloatExpression fExpr, FunctionImplementationRegistry value) {
      return fExpr;
    }

    @Override
    public LogicalExpression visitLongConstant(LongExpression intExpr, FunctionImplementationRegistry registry) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitDateConstant(DateExpression intExpr, FunctionImplementationRegistry registry) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitTimeConstant(TimeExpression intExpr, FunctionImplementationRegistry registry) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitTimeStampConstant(TimeStampExpression intExpr, FunctionImplementationRegistry registry) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitIntervalYearConstant(IntervalYearExpression intExpr, FunctionImplementationRegistry registry) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitIntervalDayConstant(IntervalDayExpression intExpr, FunctionImplementationRegistry registry) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitDoubleConstant(DoubleExpression dExpr, FunctionImplementationRegistry registry) {
      return dExpr;
    }

    @Override
    public LogicalExpression visitBooleanConstant(BooleanExpression e, FunctionImplementationRegistry registry) {
      return e;
    }

    @Override
    public LogicalExpression visitQuotedStringConstant(QuotedString e, FunctionImplementationRegistry registry) {
      return e;
    }

    @Override
    public LogicalExpression visitCastExpression(CastExpression e, FunctionImplementationRegistry value){
      
      // if the cast is pointless, remove it.
      LogicalExpression input = e.getInput().accept(this,  value);

      MajorType newMajor = e.getMajorType();
      MinorType newMinor = input.getMajorType().getMinorType();
      
      if(castEqual(e.getPosition(), newMajor, input.getMajorType())) return input; // don't do pointless cast.
      
      
      if(newMinor == MinorType.LATE || newMinor == MinorType.NULL){
        // if the type still isn't fully bound, leave as cast expression.
        return new CastExpression(input, e.getMajorType(), e.getPosition());
      }else{
        // if the type is fully bound, convert to functioncall and materialze the function.
        MajorType type = e.getMajorType();
        String castFuncWithType = "cast" + type.getMinorType().name();

        List<LogicalExpression> newArgs = Lists.newArrayList();
        newArgs.add(e.getInput());  //input_expr

        //VarLen type
        if (!Types.isFixedWidthType(type)) {
          newArgs.add(new ValueExpressions.LongExpression(type.getWidth(), null));
        }
        FunctionCall fc = new FunctionCall(castFuncWithType, newArgs, e.getPosition());
        return fc.accept(this, value);   
      }
      
      
      
    }
  
  private boolean castEqual(ExpressionPosition pos, MajorType from, MajorType to){
    if(!from.getMinorType().equals(to.getMinorType())) return false;
    switch(from.getMinorType()){
    case FLOAT4:
    case FLOAT8:
    case INT:
    case BIGINT:
    case BIT:
    case TINYINT:
    case UINT1:
    case UINT2:
    case UINT4:
    case UINT8:      
      // nothing else matters.
      return true;
     
    case FIXED16CHAR:
    case FIXEDBINARY:
    case FIXEDCHAR:
      // width always matters
      this.errorCollector.addGeneralError(pos, "Casting fixed width types are not yet supported..");
      return false;
      
    case VAR16CHAR:
    case VARBINARY:
    case VARCHAR:
      if(to.getWidth() < from.getWidth() && to.getWidth() > 0){
        this.errorCollector.addGeneralError(pos, "Casting from a longer variable length type to a shorter variable length type is not currently supported.");
        return false;
      }else{
        return true;
      }

    default:
      errorCollector.addGeneralError(pos, String.format("Casting rules are unknown for type %s.", from));
      return false;
    
    }

  }
  
  }

}
