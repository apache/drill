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
import java.util.Map;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.util.DecimalScalePrecisionAddFunction;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.util.DecimalUtility;

public class DrillDecimalAddFuncHolder extends DrillSimpleFuncHolder{

  public DrillDecimalAddFuncHolder(FunctionAttributes functionAttributes, FunctionInitializer initializer) {
    super(functionAttributes, initializer);
  }

  /*
   * This function scope is used by add and subtract functions for decimal data type.
   * DecimalScalePrecisionAddFunction is used to compute the output types'
   * scale and precision
   */
  @Override
  public MajorType getReturnType(List<LogicalExpression> args) {

    TypeProtos.DataMode mode = returnValue.type.getMode();

    if (nullHandling == NullHandling.NULL_IF_NULL) {
      // if any one of the input types is nullable, then return nullable return type
      for (LogicalExpression e : args) {
        if (e.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) {
          mode = TypeProtos.DataMode.OPTIONAL;
          break;
        }
      }
    }


    /* Get the result's scale and precision. This is a function scope for add function, assert we have
     * only two inputs
     */
    assert args.size() == 2;

    DecimalScalePrecisionAddFunction outputScalePrec =
        new DecimalScalePrecisionAddFunction(args.get(0).getMajorType().getPrecision(), args.get(0).getMajorType().getScale(),
            args.get(1).getMajorType().getPrecision(), args.get(1).getMajorType().getScale());
    return (TypeProtos.MajorType.newBuilder().setMinorType(DecimalUtility.getDecimalDataType(outputScalePrec.getOutputPrecision()))
        .setScale(outputScalePrec.getOutputScale()).setPrecision(outputScalePrec.getOutputPrecision()).setMode(mode).build());
  }

  @Override
  public boolean checkPrecisionRange() {
    return true;
  }
}
