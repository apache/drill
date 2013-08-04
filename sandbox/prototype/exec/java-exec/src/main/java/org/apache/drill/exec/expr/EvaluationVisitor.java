package org.apache.drill.exec.expr;

import com.google.common.base.Charsets;
import com.sun.codemodel.*;
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
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.CodeGenerator.HoldingContainer;
import org.apache.drill.exec.expr.fn.FunctionHolder;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.physical.impl.filter.ReturnValueExpression;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.TypeHelper;

import com.google.common.base.Preconditions;

public class EvaluationVisitor extends AbstractExprVisitor<HoldingContainer, CodeGenerator<?>, RuntimeException> {

  private FunctionImplementationRegistry registry;
  
  
  public EvaluationVisitor(FunctionImplementationRegistry registry) {
    this.registry = registry;
  }

  @Override
  public HoldingContainer visitFunctionCall(FunctionCall call, CodeGenerator<?> generator) throws RuntimeException {
    HoldingContainer[] args = new HoldingContainer[call.args.size()];
    for(int i = 0; i < call.args.size(); i++){
      args[i] = call.args.get(i).accept(this, generator);
    }
    FunctionHolder holder = registry.getFunction(call);
    
    return holder.renderFunction(generator, args);
  }
  
  @Override
  public HoldingContainer visitIfExpression(IfExpression ifExpr, CodeGenerator<?> generator) throws RuntimeException {
    JBlock local = generator.getBlock();
    
    HoldingContainer output = generator.declare(ifExpr.getMajorType());
    
    JConditional jc = null;
    JBlock conditionalBlock = new JBlock();
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
    generator.getBlock().assign(out.getValue(), JExpr.lit(e.getLong()));
    return out;
  }

  @Override
  public HoldingContainer visitDoubleConstant(DoubleExpression e, CodeGenerator<?> generator) throws RuntimeException {
    HoldingContainer out = generator.declare(e.getMajorType());
    generator.getBlock().assign(out.getValue(), JExpr.lit(e.getDouble()));
    return out;
  }

  @Override
  public HoldingContainer visitBooleanConstant(BooleanExpression e, CodeGenerator<?> generator) throws RuntimeException {
    HoldingContainer out = generator.declare(e.getMajorType());
    generator.getBlock().assign(out.getValue(), JExpr.lit(e.getBoolean() ? 1 : 0));
    return out;
  }
  
  @Override
  public HoldingContainer visitUnknown(LogicalExpression e, CodeGenerator<?> generator) throws RuntimeException {
    if(e instanceof ValueVectorReadExpression){
      return visitValueVectorExpression((ValueVectorReadExpression) e, generator);
    }else if(e instanceof ValueVectorWriteExpression){
      return visitValueVectorWriteExpression((ValueVectorWriteExpression) e, generator);
    }else if(e instanceof ReturnValueExpression){
      return visitReturnValueExpression((ReturnValueExpression) e, generator);
    }else{
      return super.visitUnknown(e, generator);  
    }
    
  }

  private HoldingContainer visitValueVectorWriteExpression(ValueVectorWriteExpression e, CodeGenerator<?> generator){
    LogicalExpression child = e.getChild();
    HoldingContainer hc = child.accept(this, generator);
    JBlock block = generator.getBlock();
    
    Class<?> vvClass = TypeHelper.getValueVectorClass(child.getMajorType().getMinorType(), child.getMajorType().getMode());
    JType vvType = generator.getModel()._ref(vvClass);
    JVar vv = generator.declareClassField("vv", vvType);
    
    // get value vector in setup block.
    JVar obj = generator.getSetupBlock().decl( //
        generator.getModel()._ref(Object.class), //
        generator.getNextVar("obj"), // 
        JExpr.direct("outgoing").invoke("getValueVectorById").arg(JExpr.lit(e.getFieldId())).arg( ((JClass)vvType).dotclass()));
    generator.getSetupBlock().assign(vv, JExpr.cast(vvType, obj));
    
    if(hc.isOptional()){
      vv.invoke("getMutator").invoke("set").arg(JExpr.direct("outIndex"));
      JConditional jc = block._if(hc.getIsSet().eq(JExpr.lit(0)).not());
      block = jc._then();
    }
    if (hc.getMinorType() == TypeProtos.MinorType.VARCHAR || hc.getMinorType() == TypeProtos.MinorType.VARBINARY) {
      block.add(vv.invoke("getMutator").invoke("set").arg(JExpr.direct("outIndex")).arg(hc.getHolder()));
    } else {
      block.add(vv.invoke("getMutator").invoke("set").arg(JExpr.direct("outIndex")).arg(hc.getValue()));
    }
    return null;
  }
  
  private HoldingContainer visitValueVectorExpression(ValueVectorReadExpression e, CodeGenerator<?> generator) throws RuntimeException{
    // declare value vector
    Class<?> vvClass = TypeHelper.getValueVectorClass(e.getMajorType().getMinorType(), e.getMajorType().getMode());
    JType vvType = generator.getModel()._ref(vvClass);
    JVar vv1 = generator.declareClassField("vv", vvType);
    
    // get value vector from incoming batch and 
    // get value vector in setup block.
    JVar obj = generator.getSetupBlock().decl( //
        generator.getModel()._ref(Object.class), //
        generator.getNextVar("obj"), // 
        JExpr.direct("incoming").invoke("getValueVectorById").arg(JExpr.lit(e.getFieldId())).arg( ((JClass)vvType).dotclass()));
    generator.getSetupBlock().assign(vv1, JExpr.cast(vvType, obj));

    // evaluation work.
    HoldingContainer out = generator.declare(e.getMajorType());
    
    
    if(out.isOptional()){
      JBlock blk = generator.getBlock();
      blk.assign(out.getIsSet(), vv1.invoke("getAccessor").invoke("isSet").arg(JExpr.direct("inIndex")));
      JConditional jc = blk._if(out.getIsSet().eq(JExpr.lit(1)));
      if (e.getMajorType().getMinorType() == TypeProtos.MinorType.VARCHAR ||
          e.getMajorType().getMinorType() == TypeProtos.MinorType.VARBINARY) {
        jc._then()
            .add(vv1.invoke("getAccessor").invoke("get").arg(JExpr.direct("inIndex")).arg(out.getHolder()));
      } else {
        jc._then()
            .assign(out.getValue(), vv1.invoke("getAccessor").invoke("get").arg(JExpr.direct("inIndex")));
      }
    }else{
      if (e.getMajorType().getMinorType() == TypeProtos.MinorType.VARCHAR ||
          e.getMajorType().getMinorType() == TypeProtos.MinorType.VARBINARY) {
        generator.getBlock().add(vv1.invoke("getAccessor").invoke("get").arg(JExpr.direct("inIndex")).arg(out.getHolder()));
      } else {
        generator.getBlock().assign(out.getValue(), vv1.invoke("getAccessor").invoke("get").arg(JExpr.direct("inIndex")));
      }
    }
    return out;
  }
  
  
  private HoldingContainer visitReturnValueExpression(ReturnValueExpression e, CodeGenerator<?> generator){
    LogicalExpression child = e.getChild();
//    Preconditions.checkArgument(child.getMajorType().equals(Types.REQUIRED_BOOLEAN));
    HoldingContainer hc = child.accept(this, generator);
    generator.getBlock()._return(hc.getValue().eq(JExpr.lit(1)));
    return null;
  }
  
  @Override
  public HoldingContainer visitQuotedStringConstant(QuotedString e, CodeGenerator<?> CodeGenerator) throws RuntimeException {
    throw new UnsupportedOperationException("We don't yet support string literals as we need to build the string value holders.");
//    JExpr stringLiteral = JExpr.lit(e.value);
//    CodeGenerator.block.decl(stringLiteral.invoke("getBytes").arg(JExpr.ref(Charsets.UTF_8));
  }

}
