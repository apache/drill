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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.ConvertExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.NullExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.TypedNullConstant;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DateExpression;
import org.apache.drill.common.expression.ValueExpressions.Decimal18Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal28Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal38Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal9Expression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.IntervalDayExpression;
import org.apache.drill.common.expression.ValueExpressions.IntervalYearExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.ValueExpressions.TimeExpression;
import org.apache.drill.common.expression.ValueExpressions.TimeStampExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.compile.sig.ConstantExpressionIdentifier;
import org.apache.drill.exec.expr.ClassGenerator.BlockType;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.fn.AbstractFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.physical.impl.filter.ReturnValueExpression;
import org.apache.drill.exec.vector.ValueHolderHelper;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import com.google.common.collect.Lists;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JLabel;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

/**
 * Visitor that generates code for eval
 */
public class EvaluationVisitor {

  private final FunctionImplementationRegistry registry;

  public EvaluationVisitor(FunctionImplementationRegistry registry) {
    super();
    this.registry = registry;
  }

  public HoldingContainer addExpr(LogicalExpression e, ClassGenerator<?> generator) {

    Set<LogicalExpression> constantBoundaries;
    if (generator.getMappingSet().hasEmbeddedConstant()) {
      constantBoundaries = Collections.emptySet();
    } else {
      constantBoundaries = ConstantExpressionIdentifier.getConstantExpressionSet(e);
    }
    return e.accept(new ConstantFilter(constantBoundaries), generator);
  }

  private class EvalVisitor extends AbstractExprVisitor<HoldingContainer, ClassGenerator<?>, RuntimeException> {

    @Override
    public HoldingContainer visitFunctionCall(FunctionCall call, ClassGenerator<?> generator) throws RuntimeException {
      throw new UnsupportedOperationException("FunctionCall is not expected here. "
          + "It should have been converted to FunctionHolderExpression in materialization");
    }

    @Override
    public HoldingContainer visitBooleanOperator(BooleanOperator op,
        ClassGenerator<?> generator) throws RuntimeException {
      if (op.getName().equals("booleanAnd")) {
        return visitBooleanAnd(op, generator);
      } else if(op.getName().equals("booleanOr")) {
        return visitBooleanOr(op, generator);
      } else {
        throw new UnsupportedOperationException("BooleanOperator can only be booleanAnd, booleanOr. You are using " + op.getName());
      }
    }

    @Override
    public HoldingContainer visitFunctionHolderExpression(FunctionHolderExpression holderExpr,
        ClassGenerator<?> generator) throws RuntimeException {

      AbstractFuncHolder holder = (AbstractFuncHolder) holderExpr.getHolder();

      JVar[] workspaceVars = holder.renderStart(generator, null);

      if (holder.isNested()) {
        generator.getMappingSet().enterChild();
      }

      HoldingContainer[] args = new HoldingContainer[holderExpr.args.size()];
      for (int i = 0; i < holderExpr.args.size(); i++) {
        args[i] = holderExpr.args.get(i).accept(this, generator);
      }

      holder.renderMiddle(generator, args, workspaceVars);

      if (holder.isNested()) {
        generator.getMappingSet().exitChild();
      }

      return holder.renderEnd(generator, args, workspaceVars);
    }

    @Override
    public HoldingContainer visitIfExpression(IfExpression ifExpr, ClassGenerator<?> generator) throws RuntimeException {
      JBlock local = generator.getEvalBlock();

      HoldingContainer output = generator.declare(ifExpr.getMajorType());

      JConditional jc = null;
      JBlock conditionalBlock = new JBlock(false, false);
      IfCondition c = ifExpr.ifCondition;

      HoldingContainer holdingContainer = c.condition.accept(this, generator);
      if (jc == null) {
        if (holdingContainer.isOptional()) {
          jc = conditionalBlock._if(holdingContainer.getIsSet().eq(JExpr.lit(1)).cand(holdingContainer.getValue().eq(JExpr.lit(1))));
        } else {
          jc = conditionalBlock._if(holdingContainer.getValue().eq(JExpr.lit(1)));
        }
      } else {
        if (holdingContainer.isOptional()) {
          jc = jc._else()._if(holdingContainer.getIsSet().eq(JExpr.lit(1)).cand(holdingContainer.getValue().eq(JExpr.lit(1))));
        } else {
          jc = jc._else()._if(holdingContainer.getValue().eq(JExpr.lit(1)));
        }
      }

      generator.nestEvalBlock(jc._then());

      HoldingContainer thenExpr = c.expression.accept(this, generator);

      generator.unNestEvalBlock();

      if (thenExpr.isOptional()) {
        JConditional newCond = jc._then()._if(thenExpr.getIsSet().ne(JExpr.lit(0)));
        JBlock b = newCond._then();
        b.assign(output.getHolder(), thenExpr.getHolder());
        //b.assign(output.getIsSet(), thenExpr.getIsSet());
      } else {
        jc._then().assign(output.getHolder(), thenExpr.getHolder());
      }

      generator.nestEvalBlock(jc._else());

      HoldingContainer elseExpr = ifExpr.elseExpression.accept(this, generator);

      generator.unNestEvalBlock();

      if (elseExpr.isOptional()) {
        JConditional newCond = jc._else()._if(elseExpr.getIsSet().ne(JExpr.lit(0)));
        JBlock b = newCond._then();
        b.assign(output.getHolder(), elseExpr.getHolder());
        //b.assign(output.getIsSet(), elseExpr.getIsSet());
      } else {
        jc._else().assign(output.getHolder(), elseExpr.getHolder());
      }
      local.add(conditionalBlock);
      return output;
    }

    @Override
    public HoldingContainer visitSchemaPath(SchemaPath path, ClassGenerator<?> generator) throws RuntimeException {
      throw new UnsupportedOperationException("All schema paths should have been replaced with ValueVectorExpressions.");
    }

    @Override
    public HoldingContainer visitLongConstant(LongExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getLong()));
      return out;
    }

    @Override
    public HoldingContainer visitIntConstant(IntExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getInt()));
      return out;
    }

    @Override
    public HoldingContainer visitDateConstant(DateExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getDate()));
      return out;
    }

    @Override
    public HoldingContainer visitTimeConstant(TimeExpression e, ClassGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getTime()));
      return out;
    }

    @Override
    public HoldingContainer visitIntervalYearConstant(IntervalYearExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getIntervalYear()));
      return out;
    }

    @Override
    public HoldingContainer visitTimeStampConstant(TimeStampExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getTimeStamp()));
      return out;
    }

    @Override
    public HoldingContainer visitDoubleConstant(DoubleExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getDouble()));
      return out;
    }

    @Override
    public HoldingContainer visitBooleanConstant(BooleanExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getBoolean() ? 1 : 0));
      return out;
    }

    @Override
    public HoldingContainer visitUnknown(LogicalExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (e instanceof ValueVectorReadExpression) {
        return visitValueVectorReadExpression((ValueVectorReadExpression) e, generator);
      } else if (e instanceof ValueVectorWriteExpression) {
        return visitValueVectorWriteExpression((ValueVectorWriteExpression) e, generator);
      } else if (e instanceof ReturnValueExpression) {
        return visitReturnValueExpression((ReturnValueExpression) e, generator);
      } else if (e instanceof HoldingContainerExpression) {
        return ((HoldingContainerExpression) e).getContainer();
      } else if (e instanceof NullExpression) {
        return generator.declare(Types.optional(MinorType.INT));
      } else if (e instanceof TypedNullConstant) {
        return generator.declare(e.getMajorType());
      } else {
        return super.visitUnknown(e, generator);
      }

    }

    private HoldingContainer visitValueVectorWriteExpression(ValueVectorWriteExpression e, ClassGenerator<?> generator) {

      final LogicalExpression child = e.getChild();
      final HoldingContainer inputContainer = child.accept(this, generator);

      JBlock block = generator.getEvalBlock();
      JExpression outIndex = generator.getMappingSet().getValueWriteIndex();
      JVar vv = generator.declareVectorValueSetupAndMember(generator.getMappingSet().getOutgoing(), e.getFieldId());

      // Only when the input is a reader, use writer interface to copy value.
      // Otherwise, input is a holder and we use vv mutator to set value.
      if (inputContainer.isReader()) {
        JType writerImpl = generator.getModel()._ref(
            TypeHelper.getWriterImpl(inputContainer.getMinorType(), inputContainer.getMajorType().getMode()));
        JType writerIFace = generator.getModel()._ref(
            TypeHelper.getWriterInterface(inputContainer.getMinorType(), inputContainer.getMajorType().getMode()));
        JVar writer = generator.declareClassField("writer", writerIFace);
        generator.getSetupBlock().assign(writer, JExpr._new(writerImpl).arg(vv).arg(JExpr._null()));
        generator.getEvalBlock().add(writer.invoke("setPosition").arg(outIndex));
        String copyMethod = inputContainer.isSingularRepeated() ? "copyAsValueSingle" : "copyAsValue";
        generator.getEvalBlock().add(inputContainer.getHolder().invoke(copyMethod).arg(writer));
        if (e.isSafe()) {
          HoldingContainer outputContainer = generator.declare(Types.REQUIRED_BIT);
          JConditional ifOut = generator.getEvalBlock()._if(writer.invoke("ok"));
          ifOut._then().assign(outputContainer.getValue(), JExpr.lit(1));
          ifOut._else().assign(outputContainer.getValue(), JExpr.lit(0));
          return outputContainer;
        }
      } else {

        final JInvocation setMeth = GetSetVectorHelper.write(e.getChild().getMajorType(), vv, inputContainer, outIndex, e.isSafe() ? "setSafe" : "set");
        final String isSafeMethod = "isSafe";

        if (e.isSafe()) {
          HoldingContainer outputContainer = generator.declare(Types.REQUIRED_BIT);
          block.assign(outputContainer.getValue(), JExpr.lit(1));
          if (inputContainer.isOptional()) {
            // block._if(vv.invoke("getMutator").invoke(setMethod).arg(outIndex).not())._then().assign(outputContainer.getValue(),
            // JExpr.lit(0));
            JConditional jc = block._if(inputContainer.getIsSet().eq(JExpr.lit(0)).not());
            block = jc._then();
            jc._else()._if(vv.invoke("getMutator").invoke(isSafeMethod).arg(outIndex).not())._then()
                .assign(outputContainer.getValue(), JExpr.lit(0));
          }
          block._if(setMeth.not())._then().assign(outputContainer.getValue(), JExpr.lit(0));
          return outputContainer;
        } else {
          if (inputContainer.isOptional()) {
            // block.add(vv.invoke("getMutator").invoke(setMethod).arg(outIndex));
            JConditional jc = block._if(inputContainer.getIsSet().eq(JExpr.lit(0)).not());
            block = jc._then();
          }
          block.add(setMeth);
        }

      }

      return null;
    }

    private HoldingContainer visitValueVectorReadExpression(ValueVectorReadExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      // declare value vector

      JExpression vv1 = generator.declareVectorValueSetupAndMember(generator.getMappingSet().getIncoming(),
          e.getFieldId());
      JExpression indexVariable = generator.getMappingSet().getValueReadIndex();

      JExpression componentVariable = indexVariable.shrz(JExpr.lit(16));
      if (e.isSuperReader()) {
        vv1 = (vv1.component(componentVariable));
        indexVariable = indexVariable.band(JExpr.lit((int) Character.MAX_VALUE));
      }

      // evaluation work.
      HoldingContainer out = generator.declare(e.getMajorType());

      final boolean primitive = !Types.usesHolderForGet(e.getMajorType());
      final boolean hasReadPath = e.hasReadPath();
      final boolean complex = Types.isComplex(e.getMajorType());
      final boolean repeated = Types.isRepeated(e.getMajorType());

      int[] fieldIds = e.getFieldId().getFieldIds();
      for (int i = 1; i < fieldIds.length; i++) {

      }

      if (!hasReadPath && !complex) {
        JBlock eval = new JBlock();
        GetSetVectorHelper.read(e.getMajorType(),  vv1, eval, out, generator.getModel(), indexVariable);
        generator.getEvalBlock().add(eval);

      } else {
        JExpression vector = e.isSuperReader() ? vv1.component(componentVariable) : vv1;
        JExpression expr = vector.invoke("getAccessor").invoke("getReader");
        PathSegment seg = e.getReadPath();

        JVar isNull = null;
        boolean isNullReaderLikely = isNullReaderLikely(seg, complex || repeated);
        if (isNullReaderLikely) {
          isNull = generator.getEvalBlock().decl(generator.getModel().INT, generator.getNextVar("isNull"), JExpr.lit(0));
        }

        JLabel label = generator.getEvalBlock().label("complex");
        JBlock eval = generator.getEvalBlock().block();

        // position to the correct value.
        eval.add(expr.invoke("setPosition").arg(indexVariable));
        int listNum = 0;

        while (seg != null) {
          if (seg.isArray()) {
            // stop once we get to the last segment and the final type is neither complex nor repeated (map, list, repeated list).
            // In case of non-complex and non-repeated type, we return Holder, in stead of FieldReader.
            if (seg.isLastPath() && !complex && !repeated) {
              break;
            }

            JVar list = generator.declareClassField("list", generator.getModel()._ref(FieldReader.class));
            eval.assign(list, expr);

            // if this is an array, set a single position for the expression to
            // allow us to read the right data lower down.
            JVar desiredIndex = eval.decl(generator.getModel().INT, "desiredIndex" + listNum,
                JExpr.lit(seg.getArraySegment().getIndex()));
            // start with negative one so that we are at zero after first call
            // to next.
            JVar currentIndex = eval.decl(generator.getModel().INT, "currentIndex" + listNum, JExpr.lit(-1));

            eval._while( //
                currentIndex.lt(desiredIndex) //
                    .cand(list.invoke("next"))).body().assign(currentIndex, currentIndex.plus(JExpr.lit(1)));


            JBlock ifNoVal = eval._if(desiredIndex.ne(currentIndex))._then().block();
            if (out.isOptional()) {
              ifNoVal.assign(out.getIsSet(), JExpr.lit(0));
            }
            ifNoVal.assign(isNull,  JExpr.lit(1));
            ifNoVal._break(label);

            expr = list.invoke("reader");
            listNum++;
          } else {
            JExpression fieldName = JExpr.lit(seg.getNameSegment().getPath());
            expr = expr.invoke("reader").arg(fieldName);
          }
          seg = seg.getChild();
        }

        if (complex || repeated) {
          MajorType finalType = e.getFieldId().getFinalType();
          // //
          JVar complexReader = generator.declareClassField("reader", generator.getModel()._ref(FieldReader.class));

          if (isNullReaderLikely) {
            JConditional jc = generator.getEvalBlock()._if(isNull.eq(JExpr.lit(0)));

            JClass nrClass = generator.getModel().ref(org.apache.drill.exec.vector.complex.impl.NullReader.class);
            JExpression nullReader = nrClass.staticRef("INSTANCE");

            jc._then().assign(complexReader, expr);
            jc._else().assign(complexReader, nullReader);
          } else {
            eval.assign(complexReader, expr);
          }

          HoldingContainer hc = new HoldingContainer(e.getMajorType(), complexReader, null, null, false, true);
          return hc;
        } else {
          if (seg != null) {
            eval.add(expr.invoke("read").arg(JExpr.lit(seg.getArraySegment().getIndex())).arg(out.getHolder()));
          } else {
            eval.add(expr.invoke("read").arg(out.getHolder()));
          }
        }

      }

      return out;
    }

    /*  Check if a Path expression could produce a NullReader. A path expression will produce a null reader, when:
     *   1) It contains an array segment as non-leaf segment :  a.b[2].c.  segment [2] might produce null reader.
     *   2) It contains an array segment as leaf segment, AND the final output is complex or repeated : a.b[2], when
     *     the final type of this expression is a map, or releated list, or repeated map.
     */
    private boolean isNullReaderLikely(PathSegment seg, boolean complexOrRepeated) {
      while (seg != null) {
        if (seg.isArray() && !seg.isLastPath()) {
          return true;
        }

        if (seg.isLastPath() && complexOrRepeated) {
          return true;
        }

        seg = seg.getChild();
      }
      return false;
    }

    private HoldingContainer visitReturnValueExpression(ReturnValueExpression e, ClassGenerator<?> generator) {
      LogicalExpression child = e.getChild();
      // Preconditions.checkArgument(child.getMajorType().equals(Types.REQUIRED_BOOLEAN));
      HoldingContainer hc = child.accept(this, generator);
      if (e.isReturnTrueOnOne()) {
        generator.getEvalBlock()._return(hc.getValue().eq(JExpr.lit(1)));
      } else {
        generator.getEvalBlock()._return(hc.getValue());
      }

      return null;
    }

    @Override
    public HoldingContainer visitQuotedStringConstant(QuotedString e, ClassGenerator<?> generator)
        throws RuntimeException {
      MajorType majorType = Types.required(MinorType.VARCHAR);
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = generator.getHolderType(majorType);
      JVar var = generator.declareClassField("string", holderType);
      JExpression stringLiteral = JExpr.lit(e.value);
      JExpression buffer = generator.getMappingSet().getIncoming().invoke("getContext").invoke("getManagedBuffer");
      setup.assign(var,
          generator.getModel().ref(ValueHolderHelper.class).staticInvoke("getVarCharHolder").arg(buffer).arg(stringLiteral));
      return new HoldingContainer(majorType, var, null, null);
    }

    @Override
    public HoldingContainer visitIntervalDayConstant(IntervalDayExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      MajorType majorType = Types.required(MinorType.INTERVALDAY);
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = generator.getHolderType(majorType);
      JVar var = generator.declareClassField("intervalday", holderType);
      JExpression dayLiteral = JExpr.lit(e.getIntervalDay());
      JExpression millisLiteral = JExpr.lit(e.getIntervalMillis());
      setup.assign(
          var,
          generator.getModel().ref(ValueHolderHelper.class).staticInvoke("getIntervalDayHolder").arg(dayLiteral)
              .arg(millisLiteral));
      return new HoldingContainer(majorType, var, null, null);
    }

    @Override
    public HoldingContainer visitDecimal9Constant(Decimal9Expression e, ClassGenerator<?> generator)
        throws RuntimeException {
      MajorType majorType = e.getMajorType();
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = generator.getHolderType(majorType);
      JVar var = generator.declareClassField("dec9", holderType);
      JExpression valueLiteral = JExpr.lit(e.getIntFromDecimal());
      JExpression scaleLiteral = JExpr.lit(e.getScale());
      JExpression precisionLiteral = JExpr.lit(e.getPrecision());
      setup.assign(
          var,
          generator.getModel().ref(ValueHolderHelper.class).staticInvoke("getDecimal9Holder").arg(valueLiteral)
              .arg(scaleLiteral).arg(precisionLiteral));
      return new HoldingContainer(majorType, var, null, null);
    }

    @Override
    public HoldingContainer visitDecimal18Constant(Decimal18Expression e, ClassGenerator<?> generator)
        throws RuntimeException {
      MajorType majorType = e.getMajorType();
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = generator.getHolderType(majorType);
      JVar var = generator.declareClassField("dec18", holderType);
      JExpression valueLiteral = JExpr.lit(e.getLongFromDecimal());
      JExpression scaleLiteral = JExpr.lit(e.getScale());
      JExpression precisionLiteral = JExpr.lit(e.getPrecision());
      setup.assign(
          var,
          generator.getModel().ref(ValueHolderHelper.class).staticInvoke("getDecimal18Holder").arg(valueLiteral)
              .arg(scaleLiteral).arg(precisionLiteral));
      return new HoldingContainer(majorType, var, null, null);
    }

    @Override
    public HoldingContainer visitDecimal28Constant(Decimal28Expression e, ClassGenerator<?> generator)
        throws RuntimeException {
      MajorType majorType = e.getMajorType();
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = generator.getHolderType(majorType);
      JVar var = generator.declareClassField("dec28", holderType);
      JExpression stringLiteral = JExpr.lit(e.getBigDecimal().toString());
      setup.assign(var,
          generator.getModel().ref(ValueHolderHelper.class).staticInvoke("getDecimal28Holder").arg(stringLiteral));
      return new HoldingContainer(majorType, var, null, null);
    }

    @Override
    public HoldingContainer visitDecimal38Constant(Decimal38Expression e, ClassGenerator<?> generator)
        throws RuntimeException {
      MajorType majorType = e.getMajorType();
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = generator.getHolderType(majorType);
      JVar var = generator.declareClassField("dec38", holderType);
      JExpression stringLiteral = JExpr.lit(e.getBigDecimal().toString());
      setup.assign(var,
          generator.getModel().ref(ValueHolderHelper.class).staticInvoke("getVarCharHolder").arg(stringLiteral));
      return new HoldingContainer(majorType, var, null, null);
    }

    @Override
    public HoldingContainer visitCastExpression(CastExpression e, ClassGenerator<?> value) throws RuntimeException {
      throw new UnsupportedOperationException("CastExpression is not expected here. "
          + "It should have been converted to FunctionHolderExpression in materialization");
    }

    @Override
    public HoldingContainer visitConvertExpression(ConvertExpression e, ClassGenerator<?> value)
        throws RuntimeException {
      String convertFunctionName = e.getConvertFunction() + e.getEncodingType();

      List<LogicalExpression> newArgs = Lists.newArrayList();
      newArgs.add(e.getInput()); // input_expr

      FunctionCall fc = new FunctionCall(convertFunctionName, newArgs, e.getPosition());
      return fc.accept(this, value);
    }

    private HoldingContainer visitBooleanAnd(BooleanOperator op,
        ClassGenerator<?> generator) {

      HoldingContainer out = generator.declare(op.getMajorType());

      JLabel label = generator.getEvalBlockLabel("AndOP");
      JBlock eval = generator.getEvalBlock().block();  // enter into nested block
      generator.nestEvalBlock(eval);

      HoldingContainer arg = null;

      JExpression e = null;

      //  value of boolean "and" when one side is null
      //    p       q     p and q
      //    true    null     null
      //    false   null     false
      //    null    true     null
      //    null    false    false
      //    null    null     null
      for (int i = 0; i < op.args.size(); i++) {
        arg = op.args.get(i).accept(this, generator);

        JBlock earlyExit = null;
        if (arg.isOptional()) {
          earlyExit = eval._if(arg.getIsSet().eq(JExpr.lit(1)).cand(arg.getValue().ne(JExpr.lit(1))))._then();
          if (e == null) {
            e = arg.getIsSet();
          } else {
            e = e.mul(arg.getIsSet());
          }
        } else {
          earlyExit = eval._if(arg.getValue().ne(JExpr.lit(1)))._then();
        }

        if (out.isOptional()) {
          earlyExit.assign(out.getIsSet(), JExpr.lit(1));
        }

        earlyExit.assign(out.getValue(),  JExpr.lit(0));
        earlyExit._break(label);
      }

      if (out.isOptional()) {
        assert (e != null);

        JConditional notSetJC = eval._if(e.eq(JExpr.lit(0)));
        notSetJC._then().assign(out.getIsSet(), JExpr.lit(0));

        JBlock setBlock = notSetJC._else().block();
        setBlock.assign(out.getIsSet(), JExpr.lit(1));
        setBlock.assign(out.getValue(), JExpr.lit(1));
      } else {
        assert (e == null);
        eval.assign(out.getValue(), JExpr.lit(1)) ;
      }

      generator.unNestEvalBlock();     // exit from nested block

      return out;
    }

    private HoldingContainer visitBooleanOr(BooleanOperator op,
        ClassGenerator<?> generator) {

      HoldingContainer out = generator.declare(op.getMajorType());

      JLabel label = generator.getEvalBlockLabel("OrOP");
      JBlock eval = generator.getEvalBlock().block();
      generator.nestEvalBlock(eval);   // enter into nested block.

      HoldingContainer arg = null;

      JExpression e = null;

      //  value of boolean "or" when one side is null
      //    p       q       p and q
      //    true    null     true
      //    false   null     null
      //    null    true     true
      //    null    false    null
      //    null    null     null

      for (int i = 0; i < op.args.size(); i++) {
        arg = op.args.get(i).accept(this, generator);

        JBlock earlyExit = null;
        if (arg.isOptional()) {
          earlyExit = eval._if(arg.getIsSet().eq(JExpr.lit(1)).cand(arg.getValue().eq(JExpr.lit(1))))._then();
          if (e == null) {
            e = arg.getIsSet();
          } else {
            e = e.mul(arg.getIsSet());
          }
        } else {
          earlyExit = eval._if(arg.getValue().eq(JExpr.lit(1)))._then();
        }

        if (out.isOptional()) {
          earlyExit.assign(out.getIsSet(), JExpr.lit(1));
        }

        earlyExit.assign(out.getValue(),  JExpr.lit(1));
        earlyExit._break(label);
      }

      if (out.isOptional()) {
        assert (e != null);

        JConditional notSetJC = eval._if(e.eq(JExpr.lit(0)));
        notSetJC._then().assign(out.getIsSet(), JExpr.lit(0));

        JBlock setBlock = notSetJC._else().block();
        setBlock.assign(out.getIsSet(), JExpr.lit(1));
        setBlock.assign(out.getValue(), JExpr.lit(0));
      } else {
        assert (e == null);
        eval.assign(out.getValue(), JExpr.lit(0)) ;
      }

      generator.unNestEvalBlock();   // exit from nested block.

      return out;
    }

  }

  private class ConstantFilter extends EvalVisitor {

    private Set<LogicalExpression> constantBoundaries;

    public ConstantFilter(Set<LogicalExpression> constantBoundaries) {
      super();
      this.constantBoundaries = constantBoundaries;
    }

    @Override
    public HoldingContainer visitFunctionCall(FunctionCall e, ClassGenerator<?> generator) throws RuntimeException {
      throw new UnsupportedOperationException("FunctionCall is not expected here. "
          + "It should have been converted to FunctionHolderExpression in materialization");
    }

    @Override
    public HoldingContainer visitFunctionHolderExpression(FunctionHolderExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitFunctionHolderExpression(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitFunctionHolderExpression(e, generator).setConstant(true);
      } else {
        return super.visitFunctionHolderExpression(e, generator);
      }
    }

    @Override
    public HoldingContainer visitBooleanOperator(BooleanOperator e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitBooleanOperator(e, generator);
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitBooleanOperator(e, generator).setConstant(true);
      } else {
        return super.visitBooleanOperator(e, generator);
      }
    }
    @Override
    public HoldingContainer visitIfExpression(IfExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitIfExpression(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitIfExpression(e, generator).setConstant(true);
      } else {
        return super.visitIfExpression(e, generator);
      }
    }

    @Override
    public HoldingContainer visitSchemaPath(SchemaPath e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitSchemaPath(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitSchemaPath(e, generator).setConstant(true);
      } else {
        return super.visitSchemaPath(e, generator);
      }
    }

    @Override
    public HoldingContainer visitLongConstant(LongExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitLongConstant(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitLongConstant(e, generator).setConstant(true);
      } else {
        return super.visitLongConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDecimal9Constant(Decimal9Expression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDecimal9Constant(e, generator);
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDecimal9Constant(e, generator).setConstant(true);
      } else {
        return super.visitDecimal9Constant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDecimal18Constant(Decimal18Expression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDecimal18Constant(e, generator);
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDecimal18Constant(e, generator).setConstant(true);
      } else {
        return super.visitDecimal18Constant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDecimal28Constant(Decimal28Expression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDecimal28Constant(e, generator);
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDecimal28Constant(e, generator).setConstant(true);
      } else {
        return super.visitDecimal28Constant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDecimal38Constant(Decimal38Expression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDecimal38Constant(e, generator);
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDecimal38Constant(e, generator).setConstant(true);
      } else {
        return super.visitDecimal38Constant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitIntConstant(IntExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitIntConstant(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitIntConstant(e, generator).setConstant(true);
      } else {
        return super.visitIntConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDateConstant(DateExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDateConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDateConstant(e, generator).setConstant(true);
      } else {
        return super.visitDateConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitTimeConstant(TimeExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitTimeConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitTimeConstant(e, generator).setConstant(true);
      } else {
        return super.visitTimeConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitIntervalYearConstant(IntervalYearExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitIntervalYearConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitIntervalYearConstant(e, generator).setConstant(true);
      } else {
        return super.visitIntervalYearConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitTimeStampConstant(TimeStampExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitTimeStampConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitTimeStampConstant(e, generator).setConstant(true);
      } else {
        return super.visitTimeStampConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDoubleConstant(DoubleExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDoubleConstant(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitDoubleConstant(e, generator).setConstant(true);
      } else {
        return super.visitDoubleConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitBooleanConstant(BooleanExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitBooleanConstant(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitBooleanConstant(e, generator).setConstant(true);
      } else {
        return super.visitBooleanConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitUnknown(LogicalExpression e, ClassGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitUnknown(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitUnknown(e, generator).setConstant(true);
      } else {
        return super.visitUnknown(e, generator);
      }
    }

    @Override
    public HoldingContainer visitQuotedStringConstant(QuotedString e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitQuotedStringConstant(e, generator);
        // generator.getMappingSet().exitConstant();
        // return c;
        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitQuotedStringConstant(e, generator).setConstant(true);
      } else {
        return super.visitQuotedStringConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitIntervalDayConstant(IntervalDayExpression e, ClassGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitIntervalDayConstant(e, generator);

        return renderConstantExpression(generator, c);
      } else if (generator.getMappingSet().isWithinConstant()) {
        return super.visitIntervalDayConstant(e, generator).setConstant(true);
      } else {
        return super.visitIntervalDayConstant(e, generator);
      }
    }

    /*
     * Get a HoldingContainer for a constant expression. The returned
     * HoldingContainder will indicate it's for a constant expression.
     */
    private HoldingContainer renderConstantExpression(ClassGenerator<?> generator, HoldingContainer input) {
      JVar fieldValue = generator.declareClassField("constant", generator.getHolderType(input.getMajorType()));
      generator.getEvalBlock().assign(fieldValue, input.getHolder());
      generator.getMappingSet().exitConstant();
      return new HoldingContainer(input.getMajorType(), fieldValue, fieldValue.ref("value"), fieldValue.ref("isSet"))
          .setConstant(true);
    }
  }

}
