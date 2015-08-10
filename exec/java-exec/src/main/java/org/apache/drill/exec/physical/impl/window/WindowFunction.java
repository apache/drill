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
package org.apache.drill.exec.physical.impl.window;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.TypedFieldId;

public class WindowFunction {
  public enum Type {
    ROW_NUMBER,
    RANK,
    DENSE_RANK,
    PERCENT_RANK,
    CUME_DIST,
    LEAD,
    LAG,
    FIRST_VALUE,
    LAST_VALUE,
    NTILE;
  }

  final Type type;

  private TypedFieldId fieldId;

  WindowFunction(Type type) {
    this.type = type;
  }

  TypeProtos.MajorType getMajorType() {
    if (type == Type.CUME_DIST || type == Type.PERCENT_RANK) {
      return Types.required(TypeProtos.MinorType.FLOAT8);
    }
    return Types.required(TypeProtos.MinorType.BIGINT);
  }

  String getName() {
    return type.name().toLowerCase();
  }

  public TypedFieldId getFieldId() {
    return fieldId;
  }

  public void setFieldId(final TypedFieldId fieldId) {
    this.fieldId = fieldId;
  }

  static WindowFunction fromExpression(final LogicalExpression expr) {
    if (!(expr instanceof FunctionCall)) {
      return null;
    }

    final String name = ((FunctionCall) expr).getName();
    final Type type;
    try {
      type = Type.valueOf(name.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null; // not a window function
    }

    if (type == Type.NTILE) {
      return new NtileWinFunc();
    }

    return new WindowFunction(type);
  }

  static class NtileWinFunc extends WindowFunction {

    private int numTiles;

    public NtileWinFunc() {
      super(Type.NTILE);
    }

    public int getNumTiles() {
      return numTiles;
    }

    public void setNumTilesFromExpression(LogicalExpression numTilesExpr) {
      if ((numTilesExpr instanceof ValueExpressions.IntExpression)) {
        int nt = ((ValueExpressions.IntExpression) numTilesExpr).getInt();
        if (nt >= 0) {
          numTiles = nt;
          return;
        }
      }

      throw new IllegalArgumentException("NTILE only accepts unsigned integer argument");
    }
  }
}
