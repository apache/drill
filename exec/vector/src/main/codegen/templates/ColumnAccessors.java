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

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/accessor/ColumnAccessors.java" />
<#include "/@includes/license.ftl" />
<#macro getType drillType label>
    @Override
    public ValueType valueType() {
  <#if label == "Int">
      return ValueType.INTEGER;
  <#elseif drillType == "VarChar" || drillType == "Var16Char">
      return ValueType.STRING;
  <#else>
      return ValueType.${label?upper_case};
  </#if>
    }
</#macro>
<#macro bindReader vectorPrefix drillType isArray >
  <#if drillType = "Decimal9" || drillType == "Decimal18">
    private MajorType type;
  </#if>
    private ${vectorPrefix}${drillType}Vector.Accessor accessor;

    @Override
    public void bindVector(ValueVector vector) {
  <#if drillType = "Decimal9" || drillType == "Decimal18">
      type = vector.getField().getType();
  </#if>
      accessor = ((${vectorPrefix}${drillType}Vector) vector).getAccessor();
    }

  <#if drillType = "Decimal9" || drillType == "Decimal18">
    @Override
    public void bindVector(MajorType type, VectorAccessor va) {
      super.bindVector(type, va);
      this.type = type;
    }

 </#if>
    private ${vectorPrefix}${drillType}Vector.Accessor accessor() {
      if (vectorAccessor == null) {
        return accessor;
      } else {
        return ((${vectorPrefix}${drillType}Vector) vectorAccessor.vector()).getAccessor();
      }
    }
</#macro>
<#macro get drillType accessorType label isArray>
    @Override
    public ${accessorType} get${label}(<#if isArray>int index</#if>) {
    <#assign getObject ="getObject"/>
  <#if isArray>
    <#assign indexVar = "index"/>
  <#else>
    <#assign indexVar = ""/>
  </#if>
  <#if drillType == "VarChar" || drillType == "Var16Char" || drillType == "VarBinary">
      return accessor().get(vectorIndex.vectorIndex(${indexVar}));
  <#elseif drillType == "Decimal9" || drillType == "Decimal18">
      return DecimalUtility.getBigDecimalFromPrimitiveTypes(
                accessor().get(vectorIndex.vectorIndex(${indexVar})),
                type.getScale(),
                type.getPrecision());
  <#elseif accessorType == "BigDecimal" || accessorType == "Period">
      return accessor().${getObject}(vectorIndex.vectorIndex(${indexVar}));
  <#elseif drillType == "UInt1">
      return ((int) accessor().get(vectorIndex.vectorIndex(${indexVar}))) & 0xFF;
  <#else>
      return accessor().get(vectorIndex.vectorIndex(${indexVar}));
  </#if>
    }
  <#if drillType == "VarChar">

    @Override
    public String getString(<#if isArray>int index</#if>) {
      return new String(getBytes(${indexVar}), Charsets.UTF_8);
    }
  <#elseif drillType == "Var16Char">

    @Override
    public String getString(<#if isArray>int index</#if>) {
      return new String(getBytes(${indexVar}), Charsets.UTF_16);
    }
  </#if>
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

package org.apache.drill.exec.vector.accessor;

import java.math.BigDecimal;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.accessor.reader.BaseScalarReader;
import org.apache.drill.exec.vector.accessor.reader.BaseElementReader;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor;
import org.apache.drill.exec.vector.accessor.writer.BaseScalarWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractFixedWidthWriter.BaseFixedWidthWriter;
import org.apache.drill.exec.vector.accessor.writer.BaseVarWidthWriter;

import com.google.common.base.Charsets;

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

// This class is generated using freemarker and the ${.template_name} template.

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
    <#if drillType == "VarChar" || drillType == "Var16Char">
      <#assign accessorType = "byte[]">
      <#assign label = "Bytes">
    </#if>
    <#if ! notyet>
  //------------------------------------------------------------------------
  // ${drillType} readers and writers

  public static class ${drillType}ColumnReader extends BaseScalarReader {

    <@bindReader "" drillType false />

    <@getType drillType label />

    <@get drillType accessorType label false/>
  }

  public static class Nullable${drillType}ColumnReader extends BaseScalarReader {

    <@bindReader "Nullable" drillType false />

    <@getType drillType label />

    @Override
    public boolean isNull() {
      return accessor().isNull(vectorIndex.vectorIndex());
    }

    <@get drillType accessorType label false />
  }

  public static class Repeated${drillType}ColumnReader extends BaseElementReader {

    <@bindReader "" drillType true />

    <@getType drillType label />

    <@get drillType accessorType label true />
  }

      <#assign varWidth = drillType == "VarChar" || drillType == "Var16Char" || drillType == "VarBinary" />
      <#if varWidth>
  public static class ${drillType}ColumnWriter extends BaseVarWidthWriter {
      <#else>
  public static class ${drillType}ColumnWriter extends BaseFixedWidthWriter {
        <#if drillType = "Decimal9" || drillType == "Decimal18" ||
             drillType == "Decimal28Sparse" || drillType == "Decimal38Sparse">
    private MajorType type;
        </#if>
    private static final int VALUE_WIDTH = ${drillType}Vector.VALUE_WIDTH;
      </#if>
    private final ${drillType}Vector vector;

    public ${drillType}ColumnWriter(final ValueVector vector) {
      <#if varWidth>
      super(((${drillType}Vector) vector).getOffsetVector());
      <#else>
        <#if drillType = "Decimal9" || drillType == "Decimal18" ||
             drillType == "Decimal28Sparse" || drillType == "Decimal38Sparse">
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
      <#if accessorType == "byte[]">
        <#assign args = ", int len">
      <#else>
        <#assign args = "">
      </#if>
      <#if javaType == "char">
        <#assign putType = "short" />
        <#assign doCast = true />
      <#else>
        <#assign putType = javaType />
        <#assign doCast = (cast == "set") />
      </#if>
      <#if ! varWidth>

    </#if>
    @Override
    public final void set${label}(final ${accessorType} value${args}) {
      <#-- Must compute the write offset first; can't be inline because the
           writeOffset() function has a side effect of possibly changing the buffer
           address (bufAddr). -->
      <#if varWidth>
      final int offset = writeIndex(len);
      <#else>
      final int writeIndex = writeIndex();
      <#assign putAddr = "writeIndex * VALUE_WIDTH">
      </#if>
      <#if varWidth>
      drillBuf.setBytes(offset, value, 0, len);
      offsetsWriter.setNextOffset(offset + len);
      <#elseif drillType == "Decimal9">
      drillBuf.setInt(${putAddr},
          DecimalUtility.getDecimal9FromBigDecimal(value,
                type.getScale(), type.getPrecision()));
      <#elseif drillType == "Decimal18">
      drillBuf.setLong(${putAddr},
          DecimalUtility.getDecimal18FromBigDecimal(value,
                type.getScale(), type.getPrecision()));
      <#elseif drillType == "Decimal38Sparse">
      <#-- Hard to optimize this case. Just use the available tools. -->
      DecimalUtility.getSparseFromBigDecimal(value, vector.getBuffer(), writeIndex * VALUE_WIDTH,
               type.getScale(), type.getPrecision(), 6);
      <#elseif drillType == "Decimal28Sparse">
      <#-- Hard to optimize this case. Just use the available tools. -->
      DecimalUtility.getSparseFromBigDecimal(value, vector.getBuffer(), writeIndex * VALUE_WIDTH,
               type.getScale(), type.getPrecision(), 5);
      <#elseif drillType == "IntervalYear">
      drillBuf.setInt(${putAddr},
                value.getYears() * 12 + value.getMonths());
      <#elseif drillType == "IntervalDay">
      final int offset = ${putAddr};
      drillBuf.setInt(offset,     value.getDays());
      drillBuf.setInt(offset + 4, periodToMillis(value));
      <#elseif drillType == "Interval">
      final int offset = ${putAddr};
      drillBuf.setInt(offset,     value.getYears() * 12 + value.getMonths());
      drillBuf.setInt(offset + 4, value.getDays());
      drillBuf.setInt(offset + 8, periodToMillis(value));
      <#elseif drillType == "Float4">
      drillBuf.setInt(${putAddr}, Float.floatToRawIntBits((float) value));
      <#elseif drillType == "Float8">
      drillBuf.setLong(${putAddr}, Double.doubleToRawLongBits(value));
      <#else>
      drillBuf.set${putType?cap_first}(${putAddr}, <#if doCast>(${putType}) </#if>value);
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
    </#if>
  }

    </#if>
  </#list>
</#list>
  public static int periodToMillis(Period value) {
    return ((value.getHours() * 60 +
             value.getMinutes()) * 60 +
             value.getSeconds()) * 1000 +
           value.getMillis();
  }

<@build vv.types "Required" "Reader" />

<@build vv.types "Nullable" "Reader" />

<@build vv.types "Repeated" "Reader" />

<@build vv.types "Required" "Writer" />
}
