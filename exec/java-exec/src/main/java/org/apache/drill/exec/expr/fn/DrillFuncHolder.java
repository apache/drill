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
package org.apache.drill.exec.expr.fn;

import java.util.Arrays;
import java.util.List;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.compile.bytecode.ScalarReplacementTypes;
import org.apache.drill.exec.compile.sig.SignatureHolder;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.BlockType;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.DrillFuncHolderExpr;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

public abstract class DrillFuncHolder extends AbstractFuncHolder {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillFuncHolder.class);

  private final FunctionAttributes attributes;
  protected final FunctionTemplate.FunctionScope scope;
  protected final FunctionTemplate.NullHandling nullHandling;
  protected final FunctionTemplate.FunctionCostCategory costCategory;
  protected final boolean isBinaryCommutative;
  protected final boolean isDeterministic;
  protected final String[] registeredNames;
  protected final WorkspaceReference[] workspaceVars;
  protected final ValueReference[] parameters;
  protected final ValueReference returnValue;
  private final FunctionInitializer initializer;

  public DrillFuncHolder(
      FunctionAttributes attributes,
      FunctionInitializer initializer) {
    super();
    this.attributes = attributes;
    this.scope = attributes.getScope();
    this.nullHandling = attributes.getNullHandling();
    this.costCategory = attributes.getCostCategory();
    this.isBinaryCommutative = attributes.isBinaryCommutative();
    this.isDeterministic = attributes.isDeterministic();
    this.registeredNames = attributes.getRegisteredNames();
    this.workspaceVars = attributes.getWorkspaceVars();
    this.parameters = attributes.getParameters();
    this.returnValue = attributes.getReturnValue();
    this.initializer = initializer;
  }

  protected String meth(String methodName) {
    return meth(methodName, true);
  }

  protected String meth(String methodName, boolean required) {
    String method = initializer.getMethod(methodName);
    if (method == null) {
      if (!required) {
        return "";
      }
      throw UserException
          .functionError()
          .message("Failure while trying use function. No body found for required method %s.", methodName)
          .addContext("FunctionClass", initializer.getClassName())
          .build(logger);
    }
    return method;
  }

  @Override
  public JVar[] renderStart(ClassGenerator<?> g, HoldingContainer[] inputVariables) {
    return declareWorkspaceVariables(g);
  };

  @Override
  public void renderMiddle(ClassGenerator<?> g, HoldingContainer[] inputVariables, JVar[] workspaceJVars) {
  };

  @Override
  public abstract HoldingContainer renderEnd(ClassGenerator<?> g, HoldingContainer[] inputVariables,
      JVar[] workspaceJVars);

  @Override
  public abstract boolean isNested();

  @Override
  public FunctionHolderExpression getExpr(String name, List<LogicalExpression> args, ExpressionPosition pos) {
    return new DrillFuncHolderExpr(name, this, args, pos);
  }

  public boolean isAggregating() {
    return false;
  }

  public boolean isDeterministic() {
    return attributes.isDeterministic();
  }

  protected JVar[] declareWorkspaceVariables(ClassGenerator<?> g) {
    JVar[] workspaceJVars = new JVar[workspaceVars.length];
    for (int i = 0; i < workspaceVars.length; i++) {
      WorkspaceReference ref = workspaceVars[i];
      JType jtype = g.getModel()._ref(ref.type);

      if (ScalarReplacementTypes.CLASSES.contains(ref.type)) {
        workspaceJVars[i] = g.declareClassField("work", jtype);
        JBlock b = g.getBlock(SignatureHolder.DRILL_INIT_METHOD);
        b.assign(workspaceJVars[i], JExpr._new(jtype));
      } else {
        workspaceJVars[i] = g.declareClassField("work", jtype);
      }

      if (ref.isInject()) {
        if (UdfUtilities.INJECTABLE_GETTER_METHODS.get(ref.getType()) != null) {
          g.getBlock(BlockType.SETUP).assign(
              workspaceJVars[i],
              g.getMappingSet().getIncoming().invoke("getContext").invoke(
                  UdfUtilities.INJECTABLE_GETTER_METHODS.get(ref.getType())
              ));
        } else {
          // Invalid injectable type provided, this should have been caught in FunctionConverter
          throw new DrillRuntimeException("Invalid injectable type requested in UDF: " + ref.getType().getSimpleName());
        }
      } else {
        //g.getBlock(BlockType.SETUP).assign(workspaceJVars[i], JExpr._new(jtype));
      }
    }
    return workspaceJVars;
  }

  protected void generateBody(ClassGenerator<?> g, BlockType bt, String body, HoldingContainer[] inputVariables,
      JVar[] workspaceJVars, boolean decConstantInputOnly) {
    if (!Strings.isNullOrEmpty(body) && !body.trim().isEmpty()) {
      JBlock sub = new JBlock(true, true);
      if (decConstantInputOnly) {
        addProtectedBlock(g, sub, body, inputVariables, workspaceJVars, true);
      } else {
        addProtectedBlock(g, sub, body, null, workspaceJVars, false);
      }
      g.getBlock(bt).directStatement(String.format("/** start %s for function %s **/ ", bt.name(), registeredNames[0]));
      g.getBlock(bt).add(sub);
      g.getBlock(bt).directStatement(String.format("/** end %s for function %s **/ ", bt.name(), registeredNames[0]));
    }
  }

  protected void addProtectedBlock(ClassGenerator<?> g, JBlock sub, String body, HoldingContainer[] inputVariables,
      JVar[] workspaceJVars, boolean decConstInputOnly) {
    if (inputVariables != null) {
      for (int i = 0; i < inputVariables.length; i++) {
        if (decConstInputOnly && !inputVariables[i].isConstant()) {
          continue;
        }

        ValueReference parameter = parameters[i];
        HoldingContainer inputVariable = inputVariables[i];
        if (parameter.isFieldReader && ! inputVariable.isReader() && ! Types.isComplex(inputVariable.getMajorType())) {
          JType singularReaderClass = g.getModel()._ref(TypeHelper.getHolderReaderImpl(inputVariable.getMajorType().getMinorType(),
              inputVariable.getMajorType().getMode()));
          JType fieldReadClass = g.getModel()._ref(FieldReader.class);
          sub.decl(fieldReadClass, parameter.name, JExpr._new(singularReaderClass).arg(inputVariable.getHolder()));
        } else {
          sub.decl(inputVariable.getHolder().type(), parameter.name, inputVariable.getHolder());
        }
      }
    }

    JVar[] internalVars = new JVar[workspaceJVars.length];
    for (int i = 0; i < workspaceJVars.length; i++) {
      if (decConstInputOnly) {
        internalVars[i] = sub.decl(g.getModel()._ref(workspaceVars[i].type), workspaceVars[i].name, workspaceJVars[i]);
      } else {
        internalVars[i] = sub.decl(g.getModel()._ref(workspaceVars[i].type), workspaceVars[i].name, workspaceJVars[i]);
      }

    }

    Preconditions.checkNotNull(body);
    sub.directStatement(body);

    // reassign workspace variables back to global space.
    for (int i = 0; i < workspaceJVars.length; i++) {
      sub.assign(workspaceJVars[i], internalVars[i]);
    }
  }

  public boolean matches(MajorType returnType, List<MajorType> argTypes) {

    if (!softCompare(returnType, returnValue.type)) {
      // logger.debug(String.format("Call [%s] didn't match as return type [%s] was different than expected [%s]. ",
      // call.getDefinition().getName(), returnValue.type, call.getMajorType()));
      return false;
    }

    if (argTypes.size() != parameters.length) {
      // logger.debug(String.format("Call [%s] didn't match as the number of arguments provided [%d] were different than expected [%d]. ",
      // call.getDefinition().getName(), parameters.length, call.args.size()));
      return false;
    }

    for (int i = 0; i < parameters.length; i++) {
      if (!softCompare(parameters[i].type, argTypes.get(i))) {
        // logger.debug(String.format("Call [%s] didn't match as the argument [%s] didn't match the expected type [%s]. ",
        // call.getDefinition().getName(), arg.getMajorType(), param.type));
        return false;
      }
    }

    return true;
  }

  @Override
  public MajorType getParmMajorType(int i) {
    return this.parameters[i].type;
  }

  @Override
  public int getParamCount() {
    return this.parameters.length;
  }

  public boolean isConstant(int i) {
    return this.parameters[i].isConstant;
  }

  public boolean isFieldReader(int i) {
    return this.parameters[i].isFieldReader;
  }

  public MajorType getReturnType(List<LogicalExpression> args) {
    if (nullHandling == NullHandling.NULL_IF_NULL) {
      // if any one of the input types is nullable, then return nullable return type
      for (LogicalExpression e : args) {
        if (e.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) {
          return Types.optional(returnValue.type.getMinorType());
        }
      }
    }

    return returnValue.type;
  }

  public NullHandling getNullHandling() {
    return attributes.getNullHandling();
  }

  private boolean softCompare(MajorType a, MajorType b) {
    return Types.softEquals(a, b, getNullHandling() == NullHandling.NULL_IF_NULL);
  }

  public String[] getRegisteredNames() {
    return attributes.getRegisteredNames();
  }

  public int getCostCategory() {
    return attributes.getCostCategory().getValue();
  }

  @Override
  public String toString() {
    final int maxLen = 10;
    return this.getClass().getSimpleName()
        + " [functionNames=" + Arrays.toString(registeredNames)
        + ", returnType=" + Types.toString(returnValue.type)
        + ", nullHandling=" + nullHandling
        + ", parameters=" + (parameters != null ? Arrays.asList(parameters).subList(0, Math.min(parameters.length, maxLen)) : null) + "]";
  }

  public WorkspaceReference[] getWorkspaceVars() {
    return this.workspaceVars;
  }

  public ValueReference[] getParameters() {
    return this.parameters;
  }

  public static class ValueReference {
    MajorType type;
    String name;
    boolean isConstant = false;
    boolean isFieldReader = false;
    boolean isComplexWriter = false;

    public ValueReference(MajorType type, String name) {
      super();
      Preconditions.checkNotNull(type);
      Preconditions.checkNotNull(name);
      this.type = type;
      this.name = name;
    }

    public MajorType getType() {
      return type;
    }

    public String getName() {
      return name;
    }

    public void setConstant(boolean isConstant) {
      this.isConstant = isConstant;
    }

    @Override
    public String toString() {
      return "ValueReference [type=" + Types.toString(type) + ", name=" + name + "]";
    }

    public static ValueReference createFieldReaderRef(String name) {
      MajorType type = Types.required(MinorType.LATE);
      ValueReference ref = new ValueReference(type, name);
      ref.isFieldReader = true;

      return ref;
    }

    public static ValueReference createComplexWriterRef(String name) {
      MajorType type = Types.required(MinorType.LATE);
      ValueReference ref = new ValueReference(type, name);
      ref.isComplexWriter = true;
      return ref;
    }

    public boolean isComplexWriter() {
      return isComplexWriter;
    }

  }

  public static class WorkspaceReference {
    Class<?> type;
    String name;
    MajorType majorType;
    boolean inject;

    public WorkspaceReference(Class<?> type, String name, boolean inject) {
      super();
      Preconditions.checkNotNull(type);
      Preconditions.checkNotNull(name);
      this.type = type;
      this.name = name;
      this.inject = inject;
    }

    void setMajorType(MajorType majorType) {
      this.majorType = majorType;
    }

    public boolean isInject() {
      return inject;
    }

    public Class<?> getType() {
      return type;
    }

    public String getName() {
      return name;
    }

  }

  public boolean checkPrecisionRange() {
    return false;
  }

  public MajorType getReturnType() {
    return returnValue.type;
  }

  public ValueReference getReturnValue() {
    return returnValue;
  }

}
