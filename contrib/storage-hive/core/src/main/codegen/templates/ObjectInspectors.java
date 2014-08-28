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
<@pp.dropOutputFile />

<#list drillOI.map as entry>
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/hive/Drill${entry.drillType}ObjectInspector.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl.hive;

import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.drill.exec.expr.holders.*;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public abstract class Drill${entry.drillType}ObjectInspector extends AbstractDrillPrimitiveObjectInspector
  implements ${entry.hiveOI} {

  public Drill${entry.drillType}ObjectInspector() {
    super(TypeInfoFactory.${entry.hiveType?lower_case}TypeInfo);
  }

<#if entry.drillType == "VarChar">
  @Override
  public HiveVarcharWritable getPrimitiveWritableObject(Object o) {
    HiveVarcharWritable valW = new HiveVarcharWritable();
    valW.set(getPrimitiveJavaObject(o));
    return valW;
  }

  public static class Required extends Drill${entry.drillType}ObjectInspector {
    @Override
    public HiveVarchar getPrimitiveJavaObject(Object o) {
      VarCharHolder h = (VarCharHolder)o;
      String s = StringFunctionHelpers.toStringFromUTF8(h.start, h.end, h.buffer);
      return new HiveVarchar(s, HiveVarchar.MAX_VARCHAR_LENGTH);
    }
  }

  public static class Optional extends Drill${entry.drillType}ObjectInspector {
    @Override
    public HiveVarchar getPrimitiveJavaObject(Object o) {
      NullableVarCharHolder h = (NullableVarCharHolder)o;
      String s = h.isSet == 0 ? null : StringFunctionHelpers.toStringFromUTF8(h.start, h.end, h.buffer);
      return new HiveVarchar(s, HiveVarchar.MAX_VARCHAR_LENGTH);
    }
  }

<#elseif entry.drillType == "Var16Char">
@Override
  public Text getPrimitiveWritableObject(Object o) {
    throw new UnsupportedOperationException();
  }

  public static class Required extends Drill${entry.drillType}ObjectInspector {
    @Override
    public String getPrimitiveJavaObject(Object o){
      Var16CharHolder h = (Var16CharHolder)o;
      String s = StringFunctionHelpers.toStringFromUTF16(h.start, h.end, h.buffer);
      return s;
    }
  }

  public static class Optional extends Drill${entry.drillType}ObjectInspector {
    @Override
    public String getPrimitiveJavaObject(Object o){
      NullableVar16CharHolder h = (NullableVar16CharHolder)o;
      String s = h.isSet == 0 ? null : StringFunctionHelpers.toStringFromUTF16(h.start, h.end, h.buffer);
      return s;
    }
  }
<#elseif entry.drillType == "VarBinary">
  @Override
  public BytesWritable getPrimitiveWritableObject(Object o) {
    return new BytesWritable(getPrimitiveJavaObject(o));
  }

  public static class Required extends Drill${entry.drillType}ObjectInspector {
    @Override
    public byte[] getPrimitiveJavaObject(Object o) {
      VarBinaryHolder h = (VarBinaryHolder)o;
      byte[] buf = new byte[h.end-h.start];
      h.buffer.getBytes(h.start, buf, 0, h.end-h.start);
      return buf;
    }
  }

  public static class Optional extends Drill${entry.drillType}ObjectInspector {
    @Override
    public byte[] getPrimitiveJavaObject(Object o) {
      NullableVarBinaryHolder h = (NullableVarBinaryHolder)o;
      byte[] buf = new byte[h.end-h.start];
      h.buffer.getBytes(h.start, buf, 0, h.end-h.start);
      return buf;
    }
  }

<#elseif entry.drillType == "Bit">
  public static class Required extends Drill${entry.drillType}ObjectInspector {
    @Override
    public boolean get(Object o) {
      return ((BitHolder)o).value == 0 ? false : true;
    }
  }

  public static class Optional extends Drill${entry.drillType}ObjectInspector {
    @Override
    public boolean get(Object o) {
    return ((NullableBitHolder)o).value == 0 ? false : true;
    }
  }

  @Override
  public BooleanWritable getPrimitiveWritableObject(Object o) {
    return new BooleanWritable(get(o));
  }

  @Override
  public Boolean getPrimitiveJavaObject(Object o) {
    return new Boolean(get(o));
  }

<#elseif entry.drillType == "Decimal38Sparse">
  public HiveDecimalWritable getPrimitiveWritableObject(Object o) {
    return new HiveDecimalWritable(getPrimitiveJavaObject(o));
  }

  public static class Required extends Drill${entry.drillType}ObjectInspector{
    @Override
    public HiveDecimal getPrimitiveJavaObject(Object o){
      Decimal38SparseHolder h = (Decimal38SparseHolder) o;
      return HiveDecimal.create(DecimalUtility.getBigDecimalFromSparse(h.buffer, h.start, h.nDecimalDigits, h.scale));
    }
  }

  public static class Optional extends Drill${entry.drillType}ObjectInspector{
    @Override
    public HiveDecimal getPrimitiveJavaObject(Object o){
      NullableDecimal38SparseHolder h = (NullableDecimal38SparseHolder) o;
      return HiveDecimal.create(DecimalUtility.getBigDecimalFromSparse(h.buffer, h.start, h.nDecimalDigits, h.scale));
    }
  }

<#elseif entry.drillType == "TimeStamp">
  @Override
  public TimestampWritable getPrimitiveWritableObject(Object o) {
    return new TimestampWritable(getPrimitiveJavaObject(o));
  }

  public static class Required extends Drill${entry.drillType}ObjectInspector{
    @Override
    public java.sql.Timestamp getPrimitiveJavaObject(Object o){
      return new java.sql.Timestamp(((TimeStampHolder)o).value);
    }
  }

  public static class Optional extends Drill${entry.drillType}ObjectInspector{
    @Override
    public java.sql.Timestamp getPrimitiveJavaObject(Object o){
      return new java.sql.Timestamp(((NullableTimeStampHolder)o).value);
    }
  }

<#elseif entry.drillType == "Date">
  @Override
  public DateWritable getPrimitiveWritableObject(Object o) {
    return new DateWritable(getPrimitiveJavaObject(o));
  }

  public static class Required extends Drill${entry.drillType}ObjectInspector{
    @Override
    public java.sql.Date getPrimitiveJavaObject(Object o){
      return new java.sql.Date(((DateHolder)o).value);
    }
  }

  public static class Optional extends Drill${entry.drillType}ObjectInspector{
    @Override
    public java.sql.Date getPrimitiveJavaObject(Object o){
      return new java.sql.Date(((NullableDateHolder)o).value);
    }
  }
<#else>
<#if entry.drillType == "Int">
  @Override
  public Integer getPrimitiveJavaObject(Object o) {
    return new Integer(get(o));
  }
<#else>
  @Override
  public ${entry.javaType?cap_first} getPrimitiveJavaObject(Object o) {
    return new ${entry.javaType?cap_first}(get(o));
  }
</#if>

  @Override
  public ${entry.javaType?cap_first}Writable getPrimitiveWritableObject(Object o) {
    return new ${entry.javaType?cap_first}Writable(get(o));
  }

  public static class Required extends Drill${entry.drillType}ObjectInspector{
    @Override
    public ${entry.javaType} get(Object o){
      return((${entry.drillType}Holder)o).value;
    }
  }

  public static class Optional extends Drill${entry.drillType}ObjectInspector{
    @Override
    public ${entry.javaType} get(Object o){
      return((Nullable${entry.drillType}Holder)o).value;
    }
  }
</#if>
}

</#list>

