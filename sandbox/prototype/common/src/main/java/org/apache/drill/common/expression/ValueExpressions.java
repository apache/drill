/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.common.expression;

import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;

public class ValueExpressions {

  public static LogicalExpression getNumericExpression(String s, ExpressionPosition ep) {
    try {
      long l = Long.parseLong(s);
      return new LongExpression(l, ep);
    } catch (Exception e) {

    }

    try {
      double d = Double.parseDouble(s);
      return new DoubleExpression(d, ep);
    } catch (Exception e) {

    }

    throw new IllegalArgumentException(String.format("Unable to parse string %s as integer or floating point number.",
        s));

  }

  protected static abstract class ValueExpression<V> extends LogicalExpressionBase {
    public final V value;

    protected ValueExpression(String value, ExpressionPosition pos) {
      super(pos);
      this.value = parseValue(value);
    }

    protected abstract V parseValue(String s);

  }

  public static class BooleanExpression extends ValueExpression<Boolean> {
    
    
    public BooleanExpression(String value, ExpressionPosition pos) {
      super(value, pos);
    }

    @Override
    protected Boolean parseValue(String s) {
      return Boolean.parseBoolean(s);
    }

    @Override
    public MajorType getMajorType() {
      return Types.REQUIRED_BOOLEAN;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitBooleanConstant(this, value);
    }

    public boolean getBoolean() {
      return value;
    }

  }

  public static class DoubleExpression extends LogicalExpressionBase {
    private double d;

    private static final MajorType DOUBLE_CONSTANT = MajorType.newBuilder().setMinorType(MinorType.FLOAT8)
        .setMode(DataMode.REQUIRED).build();

    public DoubleExpression(double d, ExpressionPosition pos) {
      super(pos);
      this.d = d;
    }

    public double getDouble() {
      return d;
    }

    @Override
    public MajorType getMajorType() {
      return DOUBLE_CONSTANT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitDoubleConstant(this, value);
    }

  }

  public static class LongExpression extends LogicalExpressionBase {

    private static final MajorType LONG_CONSTANT = MajorType.newBuilder().setMinorType(MinorType.BIGINT)
        .setMode(DataMode.REQUIRED).build();

    private long l;

    public LongExpression(long l, ExpressionPosition pos) {
      super(pos);
      this.l = l;
    }

    public long getLong() {
      return l;
    }

    @Override
    public MajorType getMajorType() {
      return LONG_CONSTANT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitLongConstant(this, value);
    }
  }

  public static class QuotedString extends ValueExpression<String> {

    private static final MajorType QUOTED_STRING_CONSTANT = MajorType.newBuilder().setMinorType(MinorType.VARCHAR2)
        .setMode(DataMode.REQUIRED).build();

    public QuotedString(String value, ExpressionPosition pos) {
      super(value, pos);
    }

    @Override
    protected String parseValue(String s) {
      return s;
    }

    @Override
    public MajorType getMajorType() {
      return QUOTED_STRING_CONSTANT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitQuotedStringConstant(this, value);
    }
  }

  public static enum CollisionBehavior {
    SKIP("-"), // keep the old value.
    FAIL("!"), // give up on the record
    REPLACE("+"), // replace the old value with the new value.
    ARRAYIFY("]"), // replace the current position with an array. Then place the
                   // old and new value in the array.
    OBJECTIFY("}"), // replace the current position with a map. Give the two
                    // values names of 'old' and 'new'.
    MERGE_OVERRIDE("%"); // do your best to do a deep merge of the old and new
                         // values.

    private String identifier;

    private CollisionBehavior(String identifier) {
      this.identifier = identifier;
    }

    public static final CollisionBehavior DEFAULT = FAIL;

    public static final CollisionBehavior find(String c) {
      if (c == null || c.isEmpty())
        return DEFAULT;

      for (CollisionBehavior b : values()) {
        if (b.identifier.equals(c))
          return b;
      }
      return DEFAULT;
    }
  }
}
