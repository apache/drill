package org.apache.drill.exec.expr;

import java.util.Collections;
import java.util.Set;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.CodeGenerator.BlockType;
import org.apache.drill.exec.expr.CodeGenerator.HoldingContainer;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.physical.impl.filter.ReturnValueExpression;
import org.apache.drill.exec.record.NullExpression;
import org.apache.drill.exec.vector.ValueHolderHelper;

import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

public class EvaluationVisitor {

  private final FunctionImplementationRegistry registry;
  
  public EvaluationVisitor(FunctionImplementationRegistry registry) {
    super();
    this.registry = registry;
  }

  public HoldingContainer addExpr(LogicalExpression e, CodeGenerator<?> generator){
//    Set<LogicalExpression> constantBoundaries = ConstantExpressionIdentifier.getConstantExpressionSet(e);
    Set<LogicalExpression> constantBoundaries = Collections.emptySet();
    return e.accept(new ConstantFilter(constantBoundaries), generator);
    
  }

  private class EvalVisitor extends AbstractExprVisitor<HoldingContainer, CodeGenerator<?>, RuntimeException> {

    
    @Override
    public HoldingContainer visitFunctionCall(FunctionCall call, CodeGenerator<?> generator) throws RuntimeException {
      DrillFuncHolder holder = registry.getFunction(call);
      JVar[] workspaceVars = holder.renderStart(generator, null);

      
      if(holder.isNested()) generator.getMappingSet().enterChild();
      HoldingContainer[] args = new HoldingContainer[call.args.size()];
      for (int i = 0; i < call.args.size(); i++) {
        args[i] = call.args.get(i).accept(this, generator);
      }
      holder.renderMiddle(generator, args, workspaceVars);
      if(holder.isNested()) generator.getMappingSet().exitChild();
      return holder.renderEnd(generator, args, workspaceVars);
    }

    @Override
    public HoldingContainer visitIfExpression(IfExpression ifExpr, CodeGenerator<?> generator) throws RuntimeException {
      JBlock local = generator.getEvalBlock();

      HoldingContainer output = generator.declare(ifExpr.getMajorType());

      JConditional jc = null;
      JBlock conditionalBlock = new JBlock(false, false);
      for (IfCondition c : ifExpr.conditions) {
        HoldingContainer HoldingContainer = c.condition.accept(this, generator);
        if (jc == null) {
          if (HoldingContainer.isOptional()) {
            jc = conditionalBlock._if(HoldingContainer.getIsSet().cand(HoldingContainer.getValue()));
          } else {
            jc = conditionalBlock._if(HoldingContainer.getValue());
          }
        } else {
          if (HoldingContainer.isOptional()) {
            jc = jc._else()._if(HoldingContainer.getIsSet().cand(HoldingContainer.getValue()));
          } else {
            jc = jc._else()._if(HoldingContainer.getValue());
          }
        }

        HoldingContainer thenExpr = c.expression.accept(this, generator);
        if (thenExpr.isOptional()) {
          JConditional newCond = jc._then()._if(thenExpr.getIsSet());
          JBlock b = newCond._then();
          b.assign(output.getValue(), thenExpr.getValue());
          b.assign(output.getIsSet(), thenExpr.getIsSet());
        } else {
          jc._then().assign(output.getValue(), thenExpr.getValue());
        }

      }

      HoldingContainer elseExpr = ifExpr.elseExpression.accept(this, generator);
      if (elseExpr.isOptional()) {
        JConditional newCond = jc._else()._if(elseExpr.getIsSet());
        JBlock b = newCond._then();
        b.assign(output.getValue(), elseExpr.getValue());
        b.assign(output.getIsSet(), elseExpr.getIsSet());
      } else {
        jc._else().assign(output.getValue(), elseExpr.getValue());

      }
      local.add(conditionalBlock);
      return output;
    }

    
    @Override
    public HoldingContainer visitSchemaPath(SchemaPath path, CodeGenerator<?> generator) throws RuntimeException {
      throw new UnsupportedOperationException("All schema paths should have been replaced with ValueVectorExpressions.");
    }

    @Override
    public HoldingContainer visitLongConstant(LongExpression e, CodeGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getLong()));
      return out;
    }

    @Override
    public HoldingContainer visitDoubleConstant(DoubleExpression e, CodeGenerator<?> generator) throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getDouble()));
      return out;
    }

    @Override
    public HoldingContainer visitBooleanConstant(BooleanExpression e, CodeGenerator<?> generator)
        throws RuntimeException {
      HoldingContainer out = generator.declare(e.getMajorType());
      generator.getEvalBlock().assign(out.getValue(), JExpr.lit(e.getBoolean() ? 1 : 0));
      return out;
    }

    @Override
    public HoldingContainer visitUnknown(LogicalExpression e, CodeGenerator<?> generator) throws RuntimeException {
      if (e instanceof ValueVectorReadExpression) {
        return visitValueVectorReadExpression((ValueVectorReadExpression) e, generator);
      } else if (e instanceof ValueVectorWriteExpression) {
        return visitValueVectorWriteExpression((ValueVectorWriteExpression) e, generator);
      } else if (e instanceof ReturnValueExpression) {
        return visitReturnValueExpression((ReturnValueExpression) e, generator);
      }else if(e instanceof HoldingContainerExpression){
        return ((HoldingContainerExpression) e).getContainer();
      }else if(e instanceof NullExpression){
        return generator.declare(Types.optional(MinorType.INT));
      } else {
        return super.visitUnknown(e, generator);
      }

    }

    private HoldingContainer visitValueVectorWriteExpression(ValueVectorWriteExpression e, CodeGenerator<?> generator) {

      LogicalExpression child = e.getChild();
      HoldingContainer inputContainer = child.accept(this, generator);
      JBlock block = generator.getEvalBlock();
      JExpression outIndex = generator.getMappingSet().getValueWriteIndex();
      JVar vv = generator.declareVectorValueSetupAndMember(generator.getMappingSet().getOutgoing(), e.getFieldId());
      String setMethod = e.isSafe() ? "setSafe" : "set";
      
      JInvocation setMeth;
      if (Types.usesHolderForGet(inputContainer.getMajorType())) {
        setMeth = vv.invoke("getMutator").invoke(setMethod).arg(outIndex).arg(inputContainer.getHolder());
      }else{
        setMeth = vv.invoke("getMutator").invoke(setMethod).arg(outIndex).arg(inputContainer.getValue());
      }
      
      if(e.isSafe()){
        HoldingContainer outputContainer = generator.declare(Types.REQUIRED_BIT);
        block.assign(outputContainer.getValue(), JExpr.lit(1));
        if(inputContainer.isOptional()){
//          block._if(vv.invoke("getMutator").invoke(setMethod).arg(outIndex).not())._then().assign(outputContainer.getValue(), JExpr.lit(0));
          JConditional jc = block._if(inputContainer.getIsSet().eq(JExpr.lit(0)).not());
          block = jc._then();
        }
        block._if(setMeth.not())._then().assign(outputContainer.getValue(), JExpr.lit(0));
        return outputContainer;
      }else{
        if (inputContainer.isOptional()) {
//          block.add(vv.invoke("getMutator").invoke(setMethod).arg(outIndex));
          JConditional jc = block._if(inputContainer.getIsSet().eq(JExpr.lit(0)).not());
          block = jc._then();
        }
        block.add(setMeth);
      }
      
      return null;
    }

    private HoldingContainer visitValueVectorReadExpression(ValueVectorReadExpression e, CodeGenerator<?> generator)
        throws RuntimeException {
      // declare value vector
      
      JVar vv1 = generator.declareVectorValueSetupAndMember(generator.getMappingSet().getIncoming(), e.getFieldId());
      JExpression indexVariable = generator.getMappingSet().getValueReadIndex();

      JInvocation getValueAccessor = vv1.invoke("getAccessor").invoke("get");
      JInvocation getValueAccessor2 = vv1.invoke("getAccessor");
      if (e.isSuperReader()) {

        getValueAccessor = ((JExpression) vv1.component(indexVariable.shrz(JExpr.lit(16)))).invoke("getAccessor").invoke("get");
        getValueAccessor2 = ((JExpression) vv1.component(indexVariable.shrz(JExpr.lit(16)))).invoke("getAccessor");
        indexVariable = indexVariable.band(JExpr.lit((int) Character.MAX_VALUE));
      }

      // evaluation work.
      HoldingContainer out = generator.declare(e.getMajorType());

      if (out.isOptional()) {
        JBlock blk = generator.getEvalBlock();
        blk.assign(out.getIsSet(), getValueAccessor2.invoke("isSet").arg(indexVariable));
        JConditional jc = blk._if(out.getIsSet().eq(JExpr.lit(1)));
        if (Types.usesHolderForGet(e.getMajorType())) {
          jc._then().add(getValueAccessor.arg(indexVariable).arg(out.getHolder()));
        } else {
          jc._then().assign(out.getValue(), getValueAccessor.arg(indexVariable));
        }
      } else {
        if (Types.usesHolderForGet(e.getMajorType())) {
          generator.getEvalBlock().add(getValueAccessor.arg(indexVariable).arg(out.getHolder()));
        } else {
          generator.getEvalBlock().assign(out.getValue(), getValueAccessor.arg(indexVariable));
        }
      }
      return out;
    }

    private HoldingContainer visitReturnValueExpression(ReturnValueExpression e, CodeGenerator<?> generator) {
      LogicalExpression child = e.getChild();
      // Preconditions.checkArgument(child.getMajorType().equals(Types.REQUIRED_BOOLEAN));
      HoldingContainer hc = child.accept(this, generator);
      if(e.isReturnTrueOnOne()){
        generator.getEvalBlock()._return(hc.getValue().eq(JExpr.lit(1)));  
      }else{
        generator.getEvalBlock()._return(hc.getValue());
      }
      
      return null;
    }

    @Override
    public HoldingContainer visitQuotedStringConstant(QuotedString e, CodeGenerator<?> generator)
        throws RuntimeException {
      MajorType majorType = Types.required(MinorType.VARCHAR);
      JBlock setup = generator.getBlock(BlockType.SETUP);
      JType holderType = generator.getHolderType(majorType);
      JVar var = generator.declareClassField("string", holderType);
      JExpression stringLiteral = JExpr.lit(e.value);
      setup.assign(var, ((JClass)generator.getModel().ref(ValueHolderHelper.class)).staticInvoke("getVarCharHolder").arg(stringLiteral));
      return new HoldingContainer(majorType, var, null, null);
      
    }
  }

  private class ConstantFilter extends EvalVisitor {

    private Set<LogicalExpression> constantBoundaries;
    
    
    public ConstantFilter(Set<LogicalExpression> constantBoundaries) {
      super();
      this.constantBoundaries = constantBoundaries;
    }

    @Override
    public HoldingContainer visitFunctionCall(FunctionCall e, CodeGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitFunctionCall(e, generator);
        generator.getMappingSet().exitConstant();
        return c;
      } else {
        return super.visitFunctionCall(e, generator);
      }
    }

    @Override
    public HoldingContainer visitIfExpression(IfExpression e, CodeGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitIfExpression(e, generator);
        generator.getMappingSet().exitConstant();
        return c;
      } else {
        return super.visitIfExpression(e, generator);
      }
    }

    @Override
    public HoldingContainer visitSchemaPath(SchemaPath e, CodeGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitSchemaPath(e, generator);
        generator.getMappingSet().exitConstant();
        return c;
      } else {
        return super.visitSchemaPath(e, generator);
      }
    }

    @Override
    public HoldingContainer visitLongConstant(LongExpression e, CodeGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitLongConstant(e, generator);
        generator.getMappingSet().exitConstant();
        return c;
      } else {
        return super.visitLongConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitDoubleConstant(DoubleExpression e, CodeGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitDoubleConstant(e, generator);
        generator.getMappingSet().exitConstant();
        return c;
      } else {
        return super.visitDoubleConstant(e, generator);
      }
    }

    @Override
    public HoldingContainer visitBooleanConstant(BooleanExpression e, CodeGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitBooleanConstant(e, generator);
        generator.getMappingSet().exitConstant();
        return c;
      } else {
        return super.visitBooleanConstant(e, generator);
      }
    }

    
    @Override
    public HoldingContainer visitUnknown(LogicalExpression e, CodeGenerator<?> generator) throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitUnknown(e, generator);
        generator.getMappingSet().exitConstant();
        return c;
      } else {
        return super.visitUnknown(e, generator);
      }
    }

    @Override
    public HoldingContainer visitQuotedStringConstant(QuotedString e, CodeGenerator<?> generator)
        throws RuntimeException {
      if (constantBoundaries.contains(e)) {
        generator.getMappingSet().enterConstant();
        HoldingContainer c = super.visitQuotedStringConstant(e, generator);
        generator.getMappingSet().exitConstant();
        return c;
      } else {
        return super.visitQuotedStringConstant(e, generator);
      }
    }

  }
}
