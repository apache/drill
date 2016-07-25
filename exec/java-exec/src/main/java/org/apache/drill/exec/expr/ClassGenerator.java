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

import static org.apache.drill.exec.compile.sig.GeneratorMapping.GM;

import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.sig.CodeGeneratorArgument;
import org.apache.drill.exec.compile.sig.CodeGeneratorMethod;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.compile.sig.SignatureHolder;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.fn.DrillFuncHolder.WorkspaceReference;
import org.apache.drill.exec.record.TypedFieldId;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JFieldRef;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JLabel;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;
import org.apache.drill.exec.server.options.OptionManager;

public class ClassGenerator<T>{

  public static final GeneratorMapping DEFAULT_SCALAR_MAP = GM("doSetup", "doEval", null, null);
  public static final GeneratorMapping DEFAULT_CONSTANT_MAP = GM("doSetup", "doSetup", null, null);

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClassGenerator.class);
  public static enum BlockType {SETUP, EVAL, RESET, CLEANUP};

  private final SignatureHolder sig;
  private final EvaluationVisitor evaluationVisitor;
  private final Map<ValueVectorSetup, JVar> vvDeclaration = Maps.newHashMap();
  private final Map<String, ClassGenerator<T>> innerClasses = Maps.newHashMap();
  private final List<TypedFieldId> workspaceTypes = Lists.newArrayList();
  private final Map<WorkspaceReference, JVar> workspaceVectors = Maps.newHashMap();
  private final CodeGenerator<T> codeGenerator;

  public final JDefinedClass clazz;
  private final LinkedList<SizedJBlock>[] blocks;
  private final JCodeModel model;
  private final OptionManager optionManager;

  private int index = 0;
  private int labelIndex = 0;
  private MappingSet mappings;

  public static MappingSet getDefaultMapping() {
    return new MappingSet("inIndex", "outIndex", DEFAULT_CONSTANT_MAP, DEFAULT_SCALAR_MAP);
  }

  @SuppressWarnings("unchecked")
  ClassGenerator(CodeGenerator<T> codeGenerator, MappingSet mappingSet, SignatureHolder signature, EvaluationVisitor eval, JDefinedClass clazz, JCodeModel model, OptionManager optionManager) throws JClassAlreadyExistsException {
    this.codeGenerator = codeGenerator;
    this.clazz = clazz;
    this.mappings = mappingSet;
    this.sig = signature;
    this.evaluationVisitor = eval;
    this.model = model;
    this.optionManager = optionManager;

    blocks = (LinkedList<SizedJBlock>[]) new LinkedList[sig.size()];
    for (int i =0; i < sig.size(); i++) {
      blocks[i] = Lists.newLinkedList();
    }
    rotateBlock();

    for (SignatureHolder child : signature.getChildHolders()) {
      String innerClassName = child.getSignatureClass().getSimpleName();
      JDefinedClass innerClazz = clazz._class(Modifier.FINAL + Modifier.PRIVATE, innerClassName);
      innerClasses.put(innerClassName, new ClassGenerator<>(codeGenerator, mappingSet, child, eval, innerClazz, model, optionManager));
    }
  }

  public ClassGenerator<T> getInnerGenerator(String name) {
    ClassGenerator<T> inner = innerClasses.get(name);
    Preconditions.checkNotNull(inner);
    return inner;
  }

  public MappingSet getMappingSet() {
    return mappings;
  }

  public void setMappingSet(MappingSet mappings) {
    this.mappings = mappings;
  }

  public CodeGenerator<T> getCodeGenerator() {
    return codeGenerator;
  }

  private GeneratorMapping getCurrentMapping() {
    return mappings.getCurrentMapping();
  }

  public JBlock getBlock(String methodName) {
    JBlock blk = this.blocks[sig.get(methodName)].getLast().getBlock();
    Preconditions.checkNotNull(blk, "Requested method name of %s was not available for signature %s.",  methodName, this.sig);
    return blk;
  }

  public JBlock getBlock(BlockType type) {
    return getBlock(getCurrentMapping().getMethodName(type));
  }

  public JBlock getSetupBlock() {
    return getBlock(getCurrentMapping().getMethodName(BlockType.SETUP));
  }
  public JBlock getEvalBlock() {
    return getBlock(getCurrentMapping().getMethodName(BlockType.EVAL));
  }
  public JBlock getResetBlock() {
    return getBlock(getCurrentMapping().getMethodName(BlockType.RESET));
  }
  public JBlock getCleanupBlock() {
    return getBlock(getCurrentMapping().getMethodName(BlockType.CLEANUP));
  }

  public void nestEvalBlock(JBlock block) {
    String methodName = getCurrentMapping().getMethodName(BlockType.EVAL);
    evaluationVisitor.newScope();
    this.blocks[sig.get(methodName)].addLast(new SizedJBlock(block));
  }

  public void unNestEvalBlock() {
    String methodName = getCurrentMapping().getMethodName(BlockType.EVAL);
    evaluationVisitor.leaveScope();
    this.blocks[sig.get(methodName)].removeLast();
  }

  public JLabel getEvalBlockLabel (String prefix) {
    return getEvalBlock().label(prefix + labelIndex ++);
  }

  public JVar declareVectorValueSetupAndMember(String batchName, TypedFieldId fieldId) {
    return declareVectorValueSetupAndMember( DirectExpression.direct(batchName), fieldId);
  }

  public JVar declareVectorValueSetupAndMember(DirectExpression batchName, TypedFieldId fieldId) {
    final ValueVectorSetup setup = new ValueVectorSetup(batchName, fieldId);
//    JVar var = this.vvDeclaration.get(setup);
//    if(var != null) return var;

    Class<?> valueVectorClass = fieldId.getIntermediateClass();
    JClass vvClass = model.ref(valueVectorClass);
    JClass retClass = vvClass;
    String vectorAccess = "getValueVector";
    if (fieldId.isHyperReader()) {
      retClass = retClass.array();
      vectorAccess = "getValueVectors";
    }

    JVar vv = declareClassField("vv", retClass);
    JClass t = model.ref(SchemaChangeException.class);
    JType objClass = model.ref(Object.class);
    JBlock b = getSetupBlock();
    //JExpr.newArray(model.INT).

    JVar fieldArr = b.decl(model.INT.array(), "fieldIds" + index++, JExpr.newArray(model.INT, fieldId.getFieldIds().length));
    int[] fieldIndices = fieldId.getFieldIds();
    for (int i = 0; i < fieldIndices.length; i++) {
       b.assign(fieldArr.component(JExpr.lit(i)), JExpr.lit(fieldIndices[i]));
    }

    JInvocation invoke = batchName
        .invoke("getValueAccessorById") //
        .arg( vvClass.dotclass())
        .arg(fieldArr);

    JVar obj = b.decl( //
        objClass, //
        getNextVar("tmp"), //
        invoke.invoke(vectorAccess));

    b._if(obj.eq(JExpr._null()))._then()._throw(JExpr._new(t).arg(JExpr.lit(String.format("Failure while loading vector %s with id: %s.", vv.name(), fieldId.toString()))));
    //b.assign(vv, JExpr.cast(retClass, ((JExpression) JExpr.cast(wrapperClass, obj) ).invoke(vectorAccess)));
    b.assign(vv, JExpr.cast(retClass, obj ));
    vvDeclaration.put(setup, vv);

    return vv;
  }

  public enum BlkCreateMode {
    TRUE,  // Create new block
    FALSE, // Do not create block; put into existing block.
    TRUE_IF_BOUND // Create new block only if # of expressions added hit upper-bound (ExecConstants.CODE_GEN_EXP_IN_METHOD_SIZE)
  }

  public HoldingContainer addExpr(LogicalExpression ex) {
    // default behavior is always to put expression into new block.
    return addExpr(ex, BlkCreateMode.TRUE);
  }

  public HoldingContainer addExpr(LogicalExpression ex, BlkCreateMode mode) {
    if (mode == BlkCreateMode.TRUE || mode == BlkCreateMode.TRUE_IF_BOUND) {
      rotateBlock(mode);
    }

    for (LinkedList<SizedJBlock> b : blocks) {
      b.getLast().incCounter();
    }

    return evaluationVisitor.addExpr(ex, this);
  }

  public void rotateBlock() {
    // default behavior is always to create new block.
    rotateBlock(BlkCreateMode.TRUE);
  }

  private void rotateBlock(BlkCreateMode mode) {
    boolean blockRotated = false;
    for (LinkedList<SizedJBlock> b : blocks) {
      if (mode == BlkCreateMode.TRUE ||
          (mode == BlkCreateMode.TRUE_IF_BOUND &&
            optionManager != null &&
            b.getLast().getCount() > optionManager.getOption(ExecConstants.CODE_GEN_EXP_IN_METHOD_SIZE_VALIDATOR))) {
        b.add(new SizedJBlock(new JBlock(true, true)));
        blockRotated = true;
      }
    }
    if (blockRotated) {
      evaluationVisitor.previousExpressions.clear();
    }
  }

  void flushCode() {
    int i = 0;
    for(CodeGeneratorMethod method : sig) {
      JMethod outer = clazz.method(JMod.PUBLIC, model._ref(method.getReturnType()), method.getMethodName());
      for(CodeGeneratorArgument arg : method) {
        outer.param(arg.getType(), arg.getName());
      }
      for(Class<?> c : method.getThrowsIterable()) {
        outer._throws(model.ref(c));
      }
      outer._throws(SchemaChangeException.class);

      int methodIndex = 0;
      int exprsInMethod = 0;
      boolean isVoidMethod = method.getReturnType() == void.class;
      for(SizedJBlock sb : blocks[i++]) {
        JBlock b = sb.getBlock();
        if(!b.isEmpty()) {
          if (optionManager != null &&
              exprsInMethod > optionManager.getOption(ExecConstants.CODE_GEN_EXP_IN_METHOD_SIZE_VALIDATOR)) {
            JMethod inner = clazz.method(JMod.PRIVATE, model._ref(method.getReturnType()), method.getMethodName() + methodIndex);
            JInvocation methodCall = JExpr.invoke(inner);
            for (CodeGeneratorArgument arg : method) {
              inner.param(arg.getType(), arg.getName());
              methodCall.arg(JExpr.direct(arg.getName()));
            }
            for (Class<?> c : method.getThrowsIterable()) {
              inner._throws(model.ref(c));
            }
            inner._throws(SchemaChangeException.class);

            if (isVoidMethod) {
              outer.body().add(methodCall);
            } else {
              outer.body()._return(methodCall);
            }
            outer = inner;
            exprsInMethod = 0;
            ++methodIndex;
          }
          outer.body().add(b);
          exprsInMethod += sb.getCount();
        }
      }
    }

    for(ClassGenerator<T> child : innerClasses.values()) {
      child.flushCode();
    }
  }

  public JCodeModel getModel() {
    return model;
  }

  public String getNextVar() {
    return "v" + index++;
  }

  public String getNextVar(String prefix) {
    return prefix + index++;
  }

  public JVar declareClassField(String prefix, JType t) {
    return clazz.field(JMod.NONE, t, prefix + index++);
  }

  public JVar declareClassField(String prefix, JType t, JExpression init) {
    return clazz.field(JMod.NONE, t, prefix + index++, init);
  }

  public HoldingContainer declare(MajorType t) {
    return declare(t, true);
  }

  public HoldingContainer declare(MajorType t, boolean includeNewInstance) {
    JType holderType = getHolderType(t);
    JVar var;
    if (includeNewInstance) {
      var = getEvalBlock().decl(holderType, "out" + index, JExpr._new(holderType));
    } else {
      var = getEvalBlock().decl(holderType, "out" + index);
    }
    JFieldRef outputSet = null;
    if (t.getMode() == DataMode.OPTIONAL) {
      outputSet = var.ref("isSet");
    }
    index++;
    return new HoldingContainer(t, var, var.ref("value"), outputSet);
  }

  public List<TypedFieldId> getWorkspaceTypes() {
    return this.workspaceTypes;
  }

  public Map<WorkspaceReference, JVar> getWorkspaceVectors() {
    return this.workspaceVectors;
  }

  private static class ValueVectorSetup{
    final DirectExpression batch;
    final TypedFieldId fieldId;

    public ValueVectorSetup(DirectExpression batch, TypedFieldId fieldId) {
      super();
      this.batch = batch;
      this.fieldId = fieldId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((batch == null) ? 0 : batch.hashCode());
      result = prime * result + ((fieldId == null) ? 0 : fieldId.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ValueVectorSetup other = (ValueVectorSetup) obj;
      if (batch == null) {
        if (other.batch != null) {
          return false;
        }
      } else if (!batch.equals(other.batch)) {
        return false;
      }
      if (fieldId == null) {
        if (other.fieldId != null) {
          return false;
        }
      } else if (!fieldId.equals(other.fieldId)) {
        return false;
      }
      return true;
    }

  }

  public static class HoldingContainer{
    private final JVar holder;
    private final JFieldRef value;
    private final JFieldRef isSet;
    private final MajorType type;
    private boolean isConstant;
    private final boolean singularRepeated;
    private final boolean isReader;

    public HoldingContainer(MajorType t, JVar holder, JFieldRef value, JFieldRef isSet) {
      this(t, holder, value, isSet, false, false);
    }

    public HoldingContainer(MajorType t, JVar holder, JFieldRef value, JFieldRef isSet, boolean singularRepeated, boolean isReader) {
      this.holder = holder;
      this.value = value;
      this.isSet = isSet;
      this.type = t;
      this.isConstant = false;
      this.singularRepeated = singularRepeated;
      this.isReader = isReader;
    }

    public boolean isReader() {
      return this.isReader;
    }

    public boolean isSingularRepeated() {
      return singularRepeated;
    }

    public HoldingContainer setConstant(boolean isConstant) {
      this.isConstant = isConstant;
      return this;
    }

    public JFieldRef f(String name) {
      return holder.ref(name);
    }

    public boolean isConstant() {
      return this.isConstant;
    }

    public JVar getHolder() {
      return holder;
    }

    public JFieldRef getValue() {
      return value;
    }

    public MajorType getMajorType() {
      return type;
    }

    public JFieldRef getIsSet() {
      Preconditions.checkNotNull(isSet, "You cannot access the isSet variable when operating on a non-nullable output value.");
      return isSet;
    }

    public boolean isOptional() {
      return type.getMode() == DataMode.OPTIONAL;
    }

    public boolean isRepeated() {
      return type.getMode() == DataMode.REPEATED;
    }

    public TypeProtos.MinorType getMinorType() {
      return type.getMinorType();
    }
  }

  public JType getHolderType(MajorType t) {
    return TypeHelper.getHolderType(model, t.getMinorType(), t.getMode());
  }

}
