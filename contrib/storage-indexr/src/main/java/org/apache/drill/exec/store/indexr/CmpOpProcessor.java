/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.indexr;

import com.google.common.collect.Sets;

import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Set;

import io.indexr.segment.ColumnType;

public class CmpOpProcessor extends AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException> {
  private boolean setNum = false;
  private long numValue;
  private String strValue;
  private SchemaPath path;
  private String functionName;
  private boolean switchDirection = false;

  private static final Set<Class<? extends LogicalExpression>> VALUE_EXPRESSION_CLASSES = Sets.newHashSet(
      ValueExpressions.BooleanExpression.class,
      ValueExpressions.DateExpression.class,
      ValueExpressions.DoubleExpression.class,
      ValueExpressions.FloatExpression.class,
      ValueExpressions.IntExpression.class,
      ValueExpressions.LongExpression.class,
      ValueExpressions.QuotedString.class,
      ValueExpressions.TimeExpression.class
  );

  public CmpOpProcessor() {}

  public long getNumValue(byte type) {
    if (setNum) {
      return numValue;
    }
    switch (type) {
      case ColumnType.INT:
        return Integer.parseInt(strValue);
      case ColumnType.LONG:
        return Long.parseLong(strValue);
      case ColumnType.FLOAT:
        return Double.doubleToRawLongBits(Float.parseFloat(strValue));
      case ColumnType.DOUBLE:
        return Double.doubleToRawLongBits(Double.parseDouble(strValue));
      case ColumnType.STRING:
        return numValue;
      default:
        throw new IllegalStateException("unsupported type " + type);
    }
  }

  public String getStrValue() {
    return strValue;
  }

  public UTF8String getUTF8StrValue() {
    return strValue == null ? null : UTF8String.fromString(strValue);
  }

  public SchemaPath getPath() {
    return path;
  }

  public boolean isSwitchDirection() {
    return switchDirection;
  }

  public String getFunctionName() {
    return functionName;
  }

  public boolean process(FunctionCall function) {
    // clear
    setNum = false;
    numValue = 0;
    strValue = null;
    path = null;

    int argSize = function.args.size();
    if (argSize != 2) {
      return false;
    }

    functionName = function.getName();
    LogicalExpression nameArg = function.args.get(0);
    LogicalExpression valueArg = function.args.get(1);

    if (VALUE_EXPRESSION_CLASSES.contains(nameArg.getClass())) {
      // "10 > a" -> "a < 10"
      switchDirection = true;
      LogicalExpression tmp = nameArg;
      nameArg = valueArg;
      valueArg = tmp;
    }

    return nameArg.accept(this, valueArg);
  }

  @Override
  public Boolean visitCastExpression(CastExpression e,
                                     LogicalExpression valueArg) throws RuntimeException {
    // Not implement yet.
    return false;
  }

  @Override
  public Boolean visitUnknown(LogicalExpression e, LogicalExpression valueArg)
      throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg)
      throws RuntimeException {
    if (valueArg instanceof ValueExpressions.QuotedString) {
      this.strValue = ((ValueExpressions.QuotedString) valueArg).value;
      this.path = path;
      return true;
    }

    if (valueArg instanceof ValueExpressions.IntExpression) {
      int value = ((ValueExpressions.IntExpression) valueArg).getInt();
      this.setNum = true;
      this.numValue = value;
      this.strValue = String.valueOf(value);
      this.path = path;
      return true;
    }

    if (valueArg instanceof ValueExpressions.LongExpression) {
      long value = ((ValueExpressions.LongExpression) valueArg).getLong();
      this.setNum = true;
      this.numValue = value;
      this.strValue = String.valueOf(value);
      this.path = path;
      return true;
    }

    if (valueArg instanceof ValueExpressions.FloatExpression) {
      float value = ((ValueExpressions.FloatExpression) valueArg).getFloat();
      this.setNum = true;
      this.numValue = Double.doubleToRawLongBits(value);
      this.strValue = String.valueOf(value);
      this.path = path;
      return true;
    }

    if (valueArg instanceof ValueExpressions.DoubleExpression) {
      double value = ((ValueExpressions.DoubleExpression) valueArg).getDouble();
      this.setNum = true;
      this.numValue = Double.doubleToRawLongBits(value);
      this.strValue = String.valueOf(value);
      this.path = path;
      return true;
    }

    if (valueArg instanceof ValueExpressions.BooleanExpression) {
      boolean value = ((ValueExpressions.BooleanExpression) valueArg).getBoolean();
      this.setNum = true;
      this.numValue = value ? 1 : 0;
      this.strValue = String.valueOf(value);
      this.path = path;
      return true;
    }

    return false;
  }
}
