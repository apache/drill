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
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.arrow.vector.types.Types.DataMode;
import org.apache.arrow.vector.types.Types.MajorType;

public class DrillDecimalZeroScaleFuncHolder extends DrillSimpleFuncHolder{

  public DrillDecimalZeroScaleFuncHolder(FunctionAttributes functionAttributes, FunctionInitializer initializer) {
    super(functionAttributes, initializer);
  }

  /* This function scope is used when we need to remove the scale part.
   * trunc and round functions with single argument use this
   */
  @Override
  public MajorType getReturnType(List<LogicalExpression> args) {

    int precision = 0;
    DataMode mode = returnValue.type.getMode();

    if (nullHandling == NullHandling.NULL_IF_NULL) {
      // if any one of the input types is nullable, then return nullable return type
      for (LogicalExpression e : args) {
        if (e.getMajorType().getMode() == DataMode.OPTIONAL) {
          mode = DataMode.OPTIONAL;
        }
        precision = Math.max(precision, e.getMajorType().getPrecision());
      }
    }

    return new MajorType(returnValue.type.getMinorType(), mode, precision, 0, 0, null);
  }
}
