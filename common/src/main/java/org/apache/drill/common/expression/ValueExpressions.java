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
package org.apache.drill.common.expression;

import java.util.GregorianCalendar;
import java.util.Iterator;

import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;

import com.google.common.collect.Iterators;

public class ValueExpressions {

  public static LogicalExpression getBigInt(long l){
    return new LongExpression(l);
  }
  
  public static LogicalExpression getInt(int i){
    return new LongExpression(i);
  }
  
  public static LogicalExpression getFloat8(double d){
    return new DoubleExpression(d, ExpressionPosition.UNKNOWN);
  }
  public static LogicalExpression getFloat4(float f){
    return new DoubleExpression(f, ExpressionPosition.UNKNOWN);
  }
  
  public static LogicalExpression getBit(boolean b){
    return new BooleanExpression(Boolean.toString(b), ExpressionPosition.UNKNOWN);
  }
  
  public static LogicalExpression getChar(String s){
    return new QuotedString(s, ExpressionPosition.UNKNOWN);
  }

  public static LogicalExpression getDate(GregorianCalendar date) {
    return new org.apache.drill.common.expression.ValueExpressions.DateExpression(date.getTimeInMillis());
  }

  public static LogicalExpression getTime(GregorianCalendar time) {
      return new TimeExpression((int) time.getTimeInMillis());
  }

  public static LogicalExpression getTimeStamp(GregorianCalendar date) {
    return new org.apache.drill.common.expression.ValueExpressions.TimeStampExpression(date.getTimeInMillis());
  }
  public static LogicalExpression getIntervalYear(int months) {
    return new IntervalYearExpression(months);
  }

  public static LogicalExpression getIntervalDay(long intervalInMillis) {
      return new IntervalDayExpression(intervalInMillis);
  }

  

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
    
    @Override
    public Iterator<LogicalExpression> iterator() {
      return Iterators.emptyIterator();
    }


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
      return Types.REQUIRED_BIT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitBooleanConstant(this, value);
    }

    public boolean getBoolean() {
      return value;
    }

  }

  public static class FloatExpression extends LogicalExpressionBase {
    private float f;

    private static final MajorType FLOAT_CONSTANT = Types.required(MinorType.FLOAT4);

    public FloatExpression(float f, ExpressionPosition pos) {
      super(pos);
      this.f = f;
    }

    public float getFloat() {
      return f;
    }

    @Override
    public MajorType getMajorType() {
      return FLOAT_CONSTANT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitFloatConstant(this, value);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Iterators.emptyIterator();
    }

  }

  public static class IntExpression extends LogicalExpressionBase {

    private static final MajorType INT_CONSTANT = Types.required(MinorType.INT);

    private int i;

    public IntExpression(int i, ExpressionPosition pos) {
      super(pos);
      this.i = i;
    }

    public int getInt() {
      return i;
    }

    @Override
    public MajorType getMajorType() {
      return INT_CONSTANT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitIntConstant(this, value);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Iterators.emptyIterator();
    }

  }

  public static class DoubleExpression extends LogicalExpressionBase {
    private double d;

    private static final MajorType DOUBLE_CONSTANT = Types.required(MinorType.FLOAT8);

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

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Iterators.emptyIterator();
    }

  }

  public static class LongExpression extends LogicalExpressionBase {

    private static final MajorType LONG_CONSTANT = Types.required(MinorType.BIGINT);

    private long l;

    public LongExpression(long l) {
      this(l, ExpressionPosition.UNKNOWN);
    }
    
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
    
    @Override
    public Iterator<LogicalExpression> iterator() {
      return Iterators.emptyIterator();
    }

  }


  public static class DateExpression extends LogicalExpressionBase {

    private static final MajorType DATE_CONSTANT = Types.required(MinorType.DATE);

    private long dateInMillis;

    public DateExpression(long l) {
      this(l, ExpressionPosition.UNKNOWN);
    }

      public DateExpression(long dateInMillis, ExpressionPosition pos) {
      super(pos);
      this.dateInMillis = dateInMillis;
    }

    public long getDate() {
      return dateInMillis;
    }

    @Override
    public MajorType getMajorType() {
      return DATE_CONSTANT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitDateConstant(this, value);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Iterators.emptyIterator();
    }

  }


  public static class TimeExpression extends LogicalExpressionBase {

    private static final MajorType TIME_CONSTANT = Types.required(MinorType.TIME);

    private int timeInMillis;

    public TimeExpression(int timeInMillis) {
      this(timeInMillis, ExpressionPosition.UNKNOWN);
    }

      public TimeExpression(int timeInMillis, ExpressionPosition pos) {
      super(pos);
      this.timeInMillis = timeInMillis;
    }

    public int getTime() {
      return timeInMillis;
    }

    @Override
    public MajorType getMajorType() {
      return TIME_CONSTANT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitTimeConstant(this, value);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Iterators.emptyIterator();
    }

  }

  public static class TimeStampExpression extends LogicalExpressionBase {

    private static final MajorType TIMESTAMP_CONSTANT = Types.required(MinorType.TIMESTAMP);

    private long timeInMillis;

    public TimeStampExpression(long timeInMillis) {
      this(timeInMillis, ExpressionPosition.UNKNOWN);
    }

      public TimeStampExpression(long timeInMillis, ExpressionPosition pos) {
      super(pos);
      this.timeInMillis = timeInMillis;
    }

    public long getTimeStamp() {
      return timeInMillis;
    }

    @Override
    public MajorType getMajorType() {
      return TIMESTAMP_CONSTANT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitTimeStampConstant(this, value);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Iterators.emptyIterator();
    }

  }

  public static class IntervalYearExpression extends LogicalExpressionBase {

    private static final MajorType INTERVALYEAR_CONSTANT = Types.required(MinorType.INTERVALYEAR);

    private int months;

    public IntervalYearExpression(int months) {
      this(months, ExpressionPosition.UNKNOWN);
    }

      public IntervalYearExpression(int months, ExpressionPosition pos) {
      super(pos);
      this.months = months;
    }

    public int getIntervalYear() {
      return months;
    }

    @Override
    public MajorType getMajorType() {
      return INTERVALYEAR_CONSTANT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitIntervalYearConstant(this, value);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Iterators.emptyIterator();
    }

  }

  public static class IntervalDayExpression extends LogicalExpressionBase {

    private static final MajorType INTERVALDAY_CONSTANT = Types.required(MinorType.INTERVALDAY);
    private static final long MILLIS_IN_DAY = 1000 * 60 * 60 * 24;
    
    private int days;
    private int millis;
    
    public IntervalDayExpression(long intervalInMillis) {
      this((int) (intervalInMillis / MILLIS_IN_DAY), (int) (intervalInMillis % MILLIS_IN_DAY), ExpressionPosition.UNKNOWN);
    }

      public IntervalDayExpression(int days, int millis, ExpressionPosition pos) {
      super(pos);
      this.days = days;
      this.millis = millis;
    }

    public int getIntervalDay() {
      return days;
    }

    public int getIntervalMillis() {
        return millis;
    }

    @Override
    public MajorType getMajorType() {
      return INTERVALDAY_CONSTANT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitIntervalDayConstant(this, value);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Iterators.emptyIterator();
    }

  }

  public static class QuotedString extends ValueExpression<String> {

    private static final MajorType QUOTED_STRING_CONSTANT = Types.required(MinorType.VARCHAR);

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
