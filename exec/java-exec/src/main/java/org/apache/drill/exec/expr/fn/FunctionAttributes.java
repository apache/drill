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

import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionCostCategory;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.fn.DrillFuncHolder.ValueReference;
import org.apache.drill.exec.expr.fn.DrillFuncHolder.WorkspaceReference;

/**
 * Attributes of a function
 * Those are used in code generation and optimization.
 */
public class FunctionAttributes {
  private final FunctionScope scope;
  private final NullHandling nullHandling;
  private final boolean isBinaryCommutative;
  private final boolean isDeterministic;
  private final String[] registeredNames;
  private final ValueReference[] parameters;
  private final ValueReference returnValue;
  private final WorkspaceReference[] workspaceVars;
  private final FunctionCostCategory costCategory;

  public FunctionAttributes(FunctionScope scope, NullHandling nullHandling, boolean isBinaryCommutative,
      boolean isDeteministic, String[] registeredNames, ValueReference[] parameters, ValueReference returnValue,
      WorkspaceReference[] workspaceVars, FunctionCostCategory costCategory) {
    super();
    this.scope = scope;
    this.nullHandling = nullHandling;
    this.isBinaryCommutative = isBinaryCommutative;
    this.isDeterministic = isDeteministic;
    this.registeredNames = registeredNames;
    this.parameters = parameters;
    this.returnValue = returnValue;
    this.workspaceVars = workspaceVars;
    this.costCategory = costCategory;
  }

  public FunctionScope getScope() {
    return scope;
  }

  public NullHandling getNullHandling() {
    return nullHandling;
  }

  public boolean isBinaryCommutative() {
    return isBinaryCommutative;
  }

  @Deprecated
  public boolean isRandom() {
    return !isDeterministic;
  }

  public boolean isDeterministic() {
    return isDeterministic;
  }

  public String[] getRegisteredNames() {
    return registeredNames;
  }

  public ValueReference[] getParameters() {
    return parameters;
  }

  public ValueReference getReturnValue() {
    return returnValue;
  }

  public WorkspaceReference[] getWorkspaceVars() {
    return workspaceVars;
  }

  public FunctionCostCategory getCostCategory() {
    return costCategory;
  }


}
