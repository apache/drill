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
package org.apache.drill.exec.expr.fn;

import com.sun.codemodel.JAssignmentTarget;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JInvocation;
import org.apache.drill.exec.expr.holders.RepeatedListHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.base.Strings;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
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
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.fn.output.OutputWidthCalculator;
import org.apache.drill.exec.expr.holders.ListHolder;
import org.apache.drill.exec.expr.holders.MapHolder;
import org.apache.drill.exec.expr.holders.RepeatedMapHolder;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import java.util.Arrays;
import java.util.List;

public abstract class DrillFuncHolder extends AbstractFuncHolder {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillFuncHolder.class);

  private final FunctionAttributes attributes;
  private final FunctionInitializer initializer;

  public DrillFuncHolder(
      FunctionAttributes attributes,
      FunctionInitializer initializer) {
    this.attributes = attributes;
    this.initializer = initializer;

    checkNullHandling(attributes.getNullHandling());
  }

  /**
   * Check if function type supports provided null handling strategy.
   * <p>Keep in mind that this method is invoked in {@link #DrillFuncHolder(FunctionAttributes, FunctionInitializer)}
   * constructor so make sure not to use any state fields when overriding the method to avoid uninitialized state.</p>
   *
   * @param nullHandling null handling strategy defined for a function
   * @throws IllegalArgumentException if provided {@code nullHandling} is not supported
   */
  protected void checkNullHandling(NullHandling nullHandling) {
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
  public JVar[] renderStart(ClassGenerator<?> g, HoldingContainer[] inputVariables, FieldReference fieldReference) {
    return declareWorkspaceVariables(g);
  }

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

  public boolean isNiladic() {
    return attributes.isNiladic();
  }

  public boolean isInternal() {
    return attributes.isInternal();
  }

  public boolean isVarArg() {
    return attributes.isVarArg();
  }

  /**
   * Generates string representation of function input parameters:
   * PARAMETER_TYPE_1-PARAMETER_MODE_1,PARAMETER_TYPE_2-PARAMETER_MODE_2
   * Example: VARCHAR-REQUIRED,VARCHAR-OPTIONAL
   * Returns empty string if function has no input parameters.
   *
   * @return string representation of function input parameters
   */
  public String getInputParameters() {
    StringBuilder builder = new StringBuilder();
    for (ValueReference ref : attributes.getParameters()) {
      MajorType type = ref.getType();
      builder.append(",");
      builder.append(type.getMinorType().toString());
      builder.append("-");
      builder.append(type.getMode().toString());
    }
    if (isVarArg() && getParamCount() > 0) {
      builder.append("...");
    }
    return builder.length() == 0 ? builder.toString() : builder.substring(1);
  }

  /**
   * @return instance of class loader used to load function
   */
  public ClassLoader getClassLoader() {
    return initializer.getClassLoader();
  }

  protected JVar[] declareWorkspaceVariables(ClassGenerator<?> g) {
    JVar[] workspaceJVars = new JVar[attributes.getWorkspaceVars().length];
    for (int i = 0; i < attributes.getWorkspaceVars().length; i++) {
      WorkspaceReference ref = attributes.getWorkspaceVars()[i];
      JType jtype = g.getModel()._ref(ref.getType());

      if (ScalarReplacementTypes.CLASSES.contains(ref.getType())) {
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
      g.getBlock(bt).directStatement(String.format("/** start %s for function %s **/ ", bt.name(), attributes.getRegisteredNames()[0]));
      g.getBlock(bt).add(sub);
      g.getBlock(bt).directStatement(String.format("/** end %s for function %s **/ ", bt.name(), attributes.getRegisteredNames()[0]));
    }
  }

  protected void addProtectedBlock(ClassGenerator<?> g, JBlock sub, String body, HoldingContainer[] inputVariables,
      JVar[] workspaceJVars, boolean decConstInputOnly) {
    if (inputVariables != null) {
      if (isVarArg()) {
        declareVarArgArray(g.getModel(), sub, inputVariables);
      }
      for (int i = 0; i < inputVariables.length; i++) {
        if (decConstInputOnly && !inputVariables[i].isConstant()) {
          continue;
        }

        ValueReference parameter = getAttributeParameter(i);
        HoldingContainer inputVariable = inputVariables[i];
        if (parameter.isFieldReader() && ! inputVariable.isReader()
            && ! Types.isComplex(inputVariable.getMajorType()) && inputVariable.getMinorType() != MinorType.UNION) {
          JType singularReaderClass = g.getModel()._ref(TypeHelper.getHolderReaderImpl(inputVariable.getMajorType().getMinorType(),
              inputVariable.getMajorType().getMode()));
          JType fieldReadClass = getParamClass(g.getModel(), parameter, inputVariable.getHolder().type());
          JInvocation reader = JExpr._new(singularReaderClass).arg(inputVariable.getHolder());
          declare(sub, parameter, fieldReadClass, reader, i);
        } else if (!parameter.isFieldReader() && inputVariable.isReader() && Types.isComplex(parameter.getType())) {
          // For complex data-types (repeated maps/lists) the input to the aggregate will be a FieldReader. However, aggregate
          // functions like ANY_VALUE, will assume the input to be a RepeatedMapHolder etc. Generate boilerplate code, to map
          // from FieldReader to respective Holder.
          if (parameter.getType().getMinorType() == MinorType.MAP
              || parameter.getType().getMinorType() == MinorType.LIST) {
            JType holderClass = getParamClass(g.getModel(), parameter, inputVariable.getHolder().type());
            JAssignmentTarget holderVar = declare(sub, parameter, holderClass, JExpr._new(holderClass), i);
            sub.assign(holderVar.ref("reader"), inputVariable.getHolder());
          }
        } else {
          declare(sub, parameter, inputVariable.getHolder().type(), inputVariable.getHolder(), i);
        }
      }
    }

    JVar[] internalVars = new JVar[workspaceJVars.length];
    for (int i = 0; i < workspaceJVars.length; i++) {
      if (decConstInputOnly) {
        internalVars[i] = sub.decl(g.getModel()._ref(attributes.getWorkspaceVars()[i].getType()), attributes.getWorkspaceVars()[i].getName(), workspaceJVars[i]);
      } else {
        internalVars[i] = sub.decl(g.getModel()._ref(attributes.getWorkspaceVars()[i].getType()), attributes.getWorkspaceVars()[i].getName(), workspaceJVars[i]);
      }

    }

    Preconditions.checkNotNull(body);
    sub.directStatement(body);

    // reassign workspace variables back to global space.
    for (int i = 0; i < workspaceJVars.length; i++) {
      sub.assign(workspaceJVars[i], internalVars[i]);
    }
  }

  /**
   * Declares array for storing vararg function arguments.
   *
   * @param model          code model to generate the code
   * @param jBlock         block of code to be populated
   * @param inputVariables array of input variables for current function
   */
  protected void declareVarArgArray(JCodeModel model, JBlock jBlock, HoldingContainer[] inputVariables) {
    ValueReference parameter = getAttributeParameter(getParamCount() - 1);
    JType defaultType;
    if (inputVariables.length >= getParamCount()) {
      defaultType = inputVariables[inputVariables.length - 1].getHolder().type();
    } else if (parameter.getType().getMinorType() == MinorType.LATE) {
      defaultType = model._ref(ValueHolder.class);
    } else {
      defaultType = TypeHelper.getHolderType(model, parameter.getType().getMinorType(), parameter.getType().getMode());
    }
    JType paramClass = getParamClass(model, parameter, defaultType);
    jBlock.decl(paramClass.array(), parameter.getName(), JExpr.newArray(paramClass, inputVariables.length - getParamCount() + 1));
  }

  /**
   * Returns {@link JType} instance which corresponds to the parameter of the function.
   *
   * @param model       code model to generate the code
   * @param parameter   function parameter which determines resulting type
   * @param defaultType type to be returned for the case when parameter does not hold specific type
   * @return {@link JType} instance which corresponds to the parameter of the function
   */
  private JType getParamClass(JCodeModel model, ValueReference parameter, JType defaultType) {
    if (parameter.isFieldReader()) {
      return model._ref(FieldReader.class);
    } else if (parameter.getType().getMinorType() == MinorType.MAP) {
      if (parameter.getType().getMode() == TypeProtos.DataMode.REPEATED) {
        return model._ref(RepeatedMapHolder.class);
      } else {
        return model._ref(MapHolder.class);
      }
    } else if (parameter.getType().getMinorType() == MinorType.LIST) {
      if (parameter.getType().getMode() == TypeProtos.DataMode.REPEATED) {
        return model._ref(RepeatedListHolder.class);
      } else {
        return model._ref(ListHolder.class);
      }
    }
    return defaultType;
  }

  /**
   * Declares specified {@code paramExpression} in specified {@code jBlock}
   * and assigns it to the array component if required and / or returns declared expression.
   *
   * @param jBlock          target block where declaration is added
   * @param parameter       function parameter which should be declared
   * @param paramClass      type of the declared variable
   * @param paramExpression expression to be declared
   * @param currentIndex    index of current parameter
   * @return declared expression
   */
  protected JAssignmentTarget declare(JBlock jBlock, ValueReference parameter,
      JType paramClass, JExpression paramExpression, int currentIndex) {
    if (parameter.isVarArg()) {
      JAssignmentTarget arrayComponent = JExpr.ref(parameter.getName()).component(JExpr.lit(currentIndex - getParamCount() + 1));
      jBlock.assign(arrayComponent, paramExpression);
      return arrayComponent;
    } else {
      return jBlock.decl(paramClass, parameter.getName(), paramExpression);
    }
  }

  public boolean matches(MajorType returnType, List<MajorType> argTypes) {

    if (!softCompare(returnType, attributes.getReturnValue().getType())) {
      // logger.debug(String.format("Call [%s] didn't match as return type [%s] was different than expected [%s]. ",
      // call.getDefinition().getName(), returnValue.type, call.getMajorType()));
      return false;
    }

    if (argTypes.size() != attributes.getParameters().length) {
      // logger.debug(String.format("Call [%s] didn't match as the number of arguments provided [%d] were different than expected [%d]. ",
      // call.getDefinition().getName(), parameters.length, call.args.size()));
      return false;
    }

    for (int i = 0; i < attributes.getParameters().length; i++) {
      if (!softCompare(getAttributeParameter(i).getType(), argTypes.get(i))) {
        // logger.debug(String.format("Call [%s] didn't match as the argument [%s] didn't match the expected type [%s]. ",
        // call.getDefinition().getName(), arg.getMajorType(), param.type));
        return false;
      }
    }

    return true;
  }

  @Override
  public MajorType getParamMajorType(int i) {
    return getAttributeParameter(i).getType();
  }

  @Override
  public int getParamCount() {
    return attributes.getParameters().length;
  }

  public boolean isConstant(int i) {
    return getAttributeParameter(i).isConstant();
  }

  /**
   * Returns i-th function attribute parameter.
   * For the case when current function is vararg and specified index
   * is greater than or equals to the attributes count, the last function attribute parameter  is returnedd.
   *
   * @param i index of function attribute parameter to  be returned
   * @return i-th function attribute parameter
   */
  public ValueReference getAttributeParameter(int i) {
    if (i >= getParamCount() && attributes.isVarArg()) {
      return attributes.getParameters()[getParamCount() - 1];
    }
    return attributes.getParameters()[i];
  }

  public boolean isFieldReader(int i) {
    return getAttributeParameter(i).isFieldReader();
  }

  public MajorType getReturnType(final List<LogicalExpression> logicalExpressions) {
    return attributes.getReturnType().getType(logicalExpressions, attributes);
  }

  public OutputWidthCalculator getOutputWidthCalculator() {
    return attributes.getOutputWidthCalculatorType().getOutputWidthCalculator();
  }

  public int variableOutputSizeEstimate(){
    return attributes.variableOutputSizeEstimate();
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

  public ValueReference[] getParameters() {
    return attributes.getParameters();
  }

  public boolean checkPrecisionRange() {
    return attributes.checkPrecisionRange();
  }

  public MajorType getReturnType() {
    return attributes.getReturnValue().getType();
  }

  public ValueReference getReturnValue() {
    return attributes.getReturnValue();
  }

  public WorkspaceReference[] getWorkspaceVars() {
    return attributes.getWorkspaceVars();
  }

  @Override
  public String toString() {
    final int maxLen = 10;
    return this.getClass().getSimpleName()
        + " [functionNames=" + Arrays.toString(attributes.getRegisteredNames())
        + ", returnType=" + Types.toString(attributes.getReturnValue().getType())
        + ", nullHandling=" + attributes.getNullHandling()
        + ", parameters=" + (attributes.getParameters() != null ?
        Arrays.asList(attributes.getParameters()).subList(0, Math.min(attributes.getParameters().length, maxLen)) : null) + "]";
  }
}
