<#macro copyright>
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

// This class is generated using Freemarker and the ${.template_name} template.
</#macro>
<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/accessor/ColumnAccessors.java" />
<#macro getType drillType label>
    @Override
    public ValueType valueType() {
  <#if label == "Int">
      return ValueType.INTEGER;
  <#elseif drillType == "VarChar" || drillType == "Var16Char">
      return ValueType.STRING;
  <#elseif drillType == "VarDecimal">
    return ValueType.DECIMAL;
  <#else>
      return ValueType.${label?upper_case};
  </#if>
    }
</#macro>
<#macro build types vectorType accessorType>
  <#if vectorType == "Repeated">
    <#assign fnPrefix = "Array" />
    <#assign classType = "Element" />
  <#else>
    <#assign fnPrefix = vectorType />
    <#assign classType = "Scalar" />
  </#if>
  <#if vectorType == "Required">
    <#assign vectorPrefix = "" />
  <#else>
    <#assign vectorPrefix = vectorType />
  </#if>
  public static void define${fnPrefix}${accessorType}s(
      Class<? extends Base${classType}${accessorType}> ${accessorType?lower_case}s[]) {
  <#list types as type>
  <#list type.minor as minor>
    <#assign drillType=minor.class>
    <#assign notyet=minor.accessorDisabled!type.accessorDisabled!false>
    <#if ! notyet>
    <#assign typeEnum=drillType?upper_case>
    ${accessorType?lower_case}s[MinorType.${typeEnum}.ordinal()] = ${vectorPrefix}${drillType}Column${accessorType}.class;
    </#if>
  </#list>
  </#list>
  }
</#macro>
<@copyright />

package org.apache.drill.exec.vector.accessor;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.vector.DateUtilities;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.accessor.reader.BaseScalarReader.BaseVarWidthReader;
import org.apache.drill.exec.vector.accessor.reader.BaseScalarReader.BaseFixedWidthReader;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor;
import org.apache.drill.exec.vector.accessor.writer.AbstractFixedWidthWriter.BaseFixedWidthWriter;
import org.apache.drill.exec.vector.accessor.writer.BaseVarWidthWriter;

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;

import io.netty.buffer.DrillBuf;

import org.joda.time.Period;

/**
 * Basic accessors for most Drill vector types and modes. These are bare-bones
 * accessors: they do only the most rudimentary type conversions. For all,
 * there is only one way to get/set values; they don't convert from, say,
 * a double to an int or visa-versa.
 * <p>
 * Writers work only with single vectors. Readers work with either single
 * vectors or a "hyper vector": a collection of vectors indexed together.
 * The details are hidden behind the {@link RowIndex} interface. If the reader
 * accesses a single vector, then the mutator is cached at bind time. However,
 * if the reader works with a hyper vector, then the vector is null at bind
 * time and must be retrieved for each row (since the vector differs row-by-
 * row.)
 */

public class ColumnAccessors {

<#list vv.types as type>
  <#list type.minor as minor>
    <#assign drillType=minor.class>
    <#assign javaType=minor.javaType!type.javaType>
    <#assign accessorType=minor.accessorType!type.accessorType!minor.friendlyType!javaType>
    <#assign label=minor.accessorLabel!type.accessorLabel!accessorType?capitalize>
    <#assign notyet=minor.accessorDisabled!type.accessorDisabled!false>
    <#assign cast=minor.accessorCast!minor.accessorCast!type.accessorCast!"none">
    <#assign friendlyType=minor.friendlyType!"">
    <#if accessorType=="BigDecimal">
      <#assign label="Decimal">
    </#if>
    <#assign varWidth = drillType == "VarChar" || drillType == "Var16Char" || drillType == "VarBinary"  || drillType == "VarDecimal"/>
    <#assign decimal = drillType == "Decimal9" || drillType == "Decimal18" ||
                       drillType == "Decimal28Sparse" || drillType == "Decimal38Sparse"  || drillType == "VarDecimal"/>
    <#if varWidth>
      <#assign accessorType = "byte[]">
      <#assign label = "Bytes">
      <#assign putArgs = ", int len">
    <#else>
      <#assign putArgs = "">
    </#if>
    <#if javaType == "char">
      <#assign putType = "short" />
      <#assign doCast = true />
    <#else>
      <#assign putType = javaType />
      <#assign doCast = (cast == "set") />
    </#if>
    <#if ! notyet>
  //------------------------------------------------------------------------
  // ${drillType} readers and writers

    <#if varWidth>
  public static class ${drillType}ColumnReader extends BaseVarWidthReader {

    <#else>
  public static class ${drillType}ColumnReader extends BaseFixedWidthReader {

    private static final int VALUE_WIDTH = ${drillType}Vector.VALUE_WIDTH;

    </#if>
    <#if decimal>
    private MajorType type;

    @Override
    public void bindVector(ColumnMetadata schema, VectorAccessor va) {
      super.bindVector(schema, va);
      <#if decimal>
      type = va.type();
      </#if>
    }

    </#if>
    <@getType drillType label />

    <#if ! varWidth>
    @Override public int width() { return VALUE_WIDTH; }

    </#if>
    @Override
    public ${accessorType} get${label}() {
    <#assign getObject ="getObject"/>
    <#assign indexVar = ""/>
      final DrillBuf buf = bufferAccessor.buffer();
    <#if ! varWidth>
      final int readOffset = vectorIndex.offset();
      <#assign getOffset = "readOffset * VALUE_WIDTH">
    </#if>
    <#if varWidth>
      final long entry = offsetsReader.getEntry();
      return buf.unsafeGetMemory((int) (entry >> 32), (int) (entry & 0xFFFF_FFFF));
    <#elseif drillType == "Decimal9">
      return DecimalUtility.getBigDecimalFromPrimitiveTypes(
          buf.getInt(${getOffset}),
          type.getScale());
    <#elseif drillType == "Decimal18">
      return DecimalUtility.getBigDecimalFromPrimitiveTypes(
          buf.getLong(${getOffset}),
          type.getScale());
    <#elseif drillType == "IntervalYear">
      return DateUtilities.fromIntervalYear(
          buf.getInt(${getOffset}));
    <#elseif drillType == "IntervalDay">
      final int offset = ${getOffset};
      return DateUtilities.fromIntervalDay(
          buf.getInt(offset),
          buf.getInt(offset + ${minor.millisecondsOffset}));
    <#elseif drillType == "Interval">
      final int offset = ${getOffset};
      return DateUtilities.fromInterval(
          buf.getInt(offset),
          buf.getInt(offset + ${minor.daysOffset}),
          buf.getInt(offset + ${minor.millisecondsOffset}));
    <#elseif drillType == "Decimal28Sparse" || drillType == "Decimal38Sparse">
      return DecimalUtility.getBigDecimalFromSparse(buf, ${getOffset},
          ${minor.nDecimalDigits}, type.getScale());
    <#elseif drillType == "Decimal28Dense" || drillType == "Decimal38Dense">
      return DecimalUtility.getBigDecimalFromDense(buf, ${getOffset},
          ${minor.nDecimalDigits}, type.getScale(),
          ${minor.maxPrecisionDigits}, VALUE_WIDTH);
    <#elseif drillType == "UInt1">
      return buf.getByte(${getOffset}) & 0xFF;
    <#elseif drillType == "UInt2">
      return buf.getShort(${getOffset}) & 0xFFFF;
    <#elseif drillType == "UInt4">
      // Should be the following:
      // return ((long) buf.unsafeGetInt(${getOffset})) & 0xFFFF_FFFF;
      // else, the unsigned values of 32 bits are mapped to negative.
      return buf.getInt(${getOffset});
    <#elseif drillType == "Float4">
      return Float.intBitsToFloat(buf.getInt(${getOffset}));
    <#elseif drillType == "Float8">
      return Double.longBitsToDouble(buf.getLong(${getOffset}));
    <#else>
      return buf.get${putType?cap_first}(${getOffset});
    </#if>
    }
  <#if drillType == "VarChar">

    @Override
    public String getString() {
      return new String(getBytes(${indexVar}), Charsets.UTF_8);
    }
  <#elseif drillType == "Var16Char">

    @Override
    public String getString() {
      return new String(getBytes(${indexVar}), Charsets.UTF_16);
    }
  <#elseif drillType == "VarDecimal">

    @Override
    public BigDecimal getDecimal() {
      byte[] bytes = getBytes();
      BigInteger unscaledValue = bytes.length == 0 ? BigInteger.ZERO : new BigInteger(bytes);
      return new BigDecimal(unscaledValue, type.getScale());
    }
  </#if>
  }

      <#if varWidth>
  public static class ${drillType}ColumnWriter extends BaseVarWidthWriter {
      <#else>
  public static class ${drillType}ColumnWriter extends BaseFixedWidthWriter {

    private static final int VALUE_WIDTH = ${drillType}Vector.VALUE_WIDTH;

        <#if decimal>
    private MajorType type;
        </#if>
      </#if>
    private final ${drillType}Vector vector;

    public ${drillType}ColumnWriter(final ValueVector vector) {
      <#if varWidth>
      super(((${drillType}Vector) vector).getOffsetVector());
      <#else>
        <#if decimal>
      type = vector.getField().getType();
        </#if>
      </#if>
      this.vector = (${drillType}Vector) vector;
    }

    @Override public BaseDataValueVector vector() { return vector; }

       <#if ! varWidth>
    @Override public int width() { return VALUE_WIDTH; }

      </#if>
      <@getType drillType label />
      <#if ! varWidth>
    </#if>

    @Override
    public final void set${label}(final ${accessorType} value${putArgs}) {
      <#-- Must compute the write offset first; can't be inline because the
           writeOffset() function has a side effect of possibly changing the buffer
           address (bufAddr). -->
      <#if ! varWidth>
      final int writeOffset = prepareWrite();
      <#assign putOffset = "writeOffset * VALUE_WIDTH">
      </#if>
      <#if varWidth>
      final int offset = prepareWrite(len);
      drillBuf.setBytes(offset, value, 0, len);
      offsetsWriter.setNextOffset(offset + len);
      <#elseif drillType == "Decimal9">
      drillBuf.setInt(${putOffset},
          DecimalUtility.getDecimal9FromBigDecimal(value,
              type.getScale()));
      <#elseif drillType == "Decimal18">
      drillBuf.setLong(${putOffset},
          DecimalUtility.getDecimal18FromBigDecimal(value,
              type.getScale()));
      <#elseif drillType == "Decimal38Sparse">
      <#-- Hard to optimize this case. Just use the available tools. -->
      DecimalUtility.getSparseFromBigDecimal(value, drillBuf,
          ${putOffset},
          type.getScale(), 6);
      <#elseif drillType == "Decimal28Sparse">
      <#-- Hard to optimize this case. Just use the available tools. -->
      DecimalUtility.getSparseFromBigDecimal(value, drillBuf,
          ${putOffset},
          type.getScale(), 5);
      <#elseif drillType == "IntervalYear">
      drillBuf.setInt(${putOffset},
          value.getYears() * 12 + value.getMonths());
      <#elseif drillType == "IntervalDay">
      final int offset = ${putOffset};
      drillBuf.setInt(offset, value.getDays());
      drillBuf.setInt(offset + ${minor.millisecondsOffset}, DateUtilities.periodToMillis(value));
      <#elseif drillType == "Interval">
      final int offset = ${putOffset};
      drillBuf.setInt(offset, DateUtilities.periodToMonths(value));
      drillBuf.setInt(offset + ${minor.daysOffset}, value.getDays());
      drillBuf.setInt(offset + ${minor.millisecondsOffset}, DateUtilities.periodToMillis(value));
      <#elseif drillType == "Float4">
      drillBuf.setInt(${putOffset}, Float.floatToRawIntBits((float) value));
      <#elseif drillType == "Float8">
      drillBuf.setLong(${putOffset}, Double.doubleToRawLongBits(value));
      <#else>
      drillBuf.set${putType?cap_first}(${putOffset}, <#if doCast>(${putType}) </#if>value);
      </#if>
      vectorIndex.nextElement();
    }
    <#if drillType == "VarChar">

    @Override
    public final void setString(String value) {
      final byte bytes[] = value.getBytes(Charsets.UTF_8);
      setBytes(bytes, bytes.length);
    }
    <#elseif drillType == "Var16Char">

    @Override
    public final void setString(String value) {
      final byte bytes[] = value.getBytes(Charsets.UTF_16);
      setBytes(bytes, bytes.length);
    }

    <#elseif drillType = "VarDecimal">

    @Override
    public final void setDecimal(final BigDecimal bd) {
      byte[] barr = bd.unscaledValue().toByteArray();
      int len = barr.length;
      setBytes(barr, len);
    }
    </#if>
  }

    </#if>
  </#list>
</#list>
}
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/accessor/ColumnAccessorUtils.java" />
<@copyright />

package org.apache.drill.exec.vector.accessor;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.accessor.ColumnAccessors.*;
import org.apache.drill.exec.vector.accessor.reader.BaseScalarReader;
import org.apache.drill.exec.vector.accessor.writer.BaseScalarWriter;

public class ColumnAccessorUtils {

  private ColumnAccessorUtils() { }

<@build vv.types "Required" "Reader" />

<@build vv.types "Required" "Writer" />
}
