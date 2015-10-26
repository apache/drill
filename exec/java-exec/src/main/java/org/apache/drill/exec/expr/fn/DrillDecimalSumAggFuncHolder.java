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

import java.util.List;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;

public class DrillDecimalSumAggFuncHolder extends DrillAggFuncHolder {

  public DrillDecimalSumAggFuncHolder(FunctionAttributes attributes, FunctionInitializer initializer) {
    super(attributes, initializer);
  }

  @Override
  public TypeProtos.MajorType getReturnType(List<LogicalExpression> args) {

    int scale = 0;
    int precision = 0;

    // Get the max scale and precision from the inputs
    for (LogicalExpression e : args) {
      scale = Math.max(scale, e.getMajorType().getScale());
      precision = Math.max(precision, e.getMajorType().getPrecision());
    }

    return (TypeProtos.MajorType.newBuilder().setMinorType(returnValue.type.getMinorType()).setScale(scale).setPrecision(38).setMode(TypeProtos.DataMode.REQUIRED).build());
  }
}
