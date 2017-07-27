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
<#macro bindWriter vectorPrefix mode drillType>
  <#if drillType = "Decimal9" || drillType == "Decimal18">
    private MajorType type;
  </#if>
    private ${vectorPrefix}${drillType}Vector.Mutator mutator;

    @Override
    public void bindVector(ValueVector vector) {
  <#if drillType = "Decimal9" || drillType == "Decimal18">
      type = vector.getField().getType();
  </#if>
      this.mutator = ((${vectorPrefix}${drillType}Vector) vector).getMutator();
    }
</#macro>
<#-- DRILL-5529 describes how required and repeated vectors don't implement
     fillEmpties() to recover from skiped writes. Since the mutator does
     not do the work, we must insert code here to do it.
     DRILL-5530 describes why required vectors may not be initialized
     to zeros. -->
<#macro fillEmpties drillType mode>
  <#if mode == "" && drillType != "Bit">
        // Work-around for DRILL-5529: lack of fillEmpties for some vectors.
        // See DRILL-5530 for why this is needed for required vectors.
        if (lastWriteIndex + 1 < writeIndex) {
          mutator.fillEmptiesBounded(lastWriteIndex, writeIndex);
        }
 </#if>
</#macro>
<#macro advance mode>
  <#if mode == "Repeated">
      vectorIndex.next();
  </#if>
</#macro>
<#macro set drillType accessorType label vectorPrefix mode setFn>
  <#if accessorType == "byte[]">
    <#assign args = ", int len">
  <#else>
    <#assign args = "">
  </#if>
    @Override
    public void set${label}(${accessorType} value${args}) throws VectorOverflowException {
      try {
        final int writeIndex = vectorIndex.vectorIndex();
        <@fillEmpties drillType mode />
  <#if drillType == "VarChar" || drillType == "Var16Char" || drillType == "VarBinary">
        mutator.${setFn}(writeIndex, value, 0, len);
  <#elseif drillType == "Decimal9">
        mutator.${setFn}(writeIndex,
            DecimalUtility.getDecimal9FromBigDecimal(value,
              type.getScale(), type.getPrecision()));
  <#elseif drillType == "Decimal18">
        mutator.${setFn}(writeIndex,
            DecimalUtility.getDecimal18FromBigDecimal(value,
                type.getScale(), type.getPrecision()));
  <#elseif drillType == "IntervalYear">
        mutator.${setFn}(writeIndex, value.getYears() * 12 + value.getMonths());
  <#elseif drillType == "IntervalDay">
        mutator.${setFn}(writeIndex,<#if mode == "Nullable"> 1,</#if>
                      value.getDays(),
                      periodToMillis(value));
  <#elseif drillType == "Interval">
        mutator.${setFn}(writeIndex,<#if mode == "Nullable"> 1,</#if>
                      value.getYears() * 12 + value.getMonths(),
                      value.getDays(), periodToMillis(value));
  <#else>
        mutator.${setFn}(writeIndex, <#if cast=="set">(${javaType}) </#if>value);
  </#if>
  <#if mode != "Repeated">
        lastWriteIndex = writeIndex;
  </#if>
      } catch (VectorOverflowException e) {
        vectorIndex.overflowed();
        throw e;
      }
      <@advance mode />
    }
  <#if drillType == "VarChar">

    @Override
    public void setString(String value) throws VectorOverflowException {
      final byte bytes[] = value.getBytes(Charsets.UTF_8);
      setBytes(bytes, bytes.length);
    }
  <#elseif drillType == "Var16Char">

    @Override
    public void setString(String value) throws VectorOverflowException {
      final byte bytes[] = value.getBytes(Charsets.UTF_8);
      setBytes(bytes, bytes.length);
    }
  </#if>
</#macro>
<#macro finishBatch drillType mode>
    @Override
    protected void finish() throws VectorOverflowException {
      final int rowCount = vectorIndex.vectorIndex();
  <#-- See note above for the fillEmpties macro. -->
  <#if mode == "" && drillType != "Bit">
      // Work-around for DRILL-5529: lack of fillEmpties for some vectors.
      // See DRILL-5530 for why this is needed for required vectors.
      mutator.fillEmptiesBounded(lastWriteIndex, rowCount - 1);
   </#if>
      mutator.setValueCount(rowCount);
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
import org.apache.drill.exec.vector.accessor.writer.BaseElementWriter;

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

  public static class ${drillType}ColumnWriter extends BaseScalarWriter {

    <@bindWriter "" "Required" drillType />

    <@getType drillType label />

    <@set drillType accessorType label "" "Required" "setScalar" />

    <@finishBatch drillType "" />
  }

  public static class Nullable${drillType}ColumnWriter extends BaseScalarWriter {

    <@bindWriter "Nullable" "Nullable" drillType />

    <@getType drillType label />

    <#-- Generated for each type because, unfortunately, there is
         no common base class for nullable vectors even though the
         null vector is generic and should be common. Instead, the
         null vector is generated as part of each kind of nullable
         vector.
         TODO: Factor out the null vector and move this code to
         the base class.
    -->
    @Override
    public void setNull() throws VectorOverflowException {
      try {
        final int writeIndex = vectorIndex.vectorIndex();
        mutator.setNullBounded(writeIndex);
        lastWriteIndex = vectorIndex.vectorIndex();
      } catch (VectorOverflowException e) {
        vectorIndex.overflowed();
        throw e;
      }
    }

    <@set drillType accessorType label "Nullable" "Nullable" "setScalar" />

    <@finishBatch drillType "Nullable" />
  }

  public static class Repeated${drillType}ColumnWriter extends BaseElementWriter {

    <@bindWriter "" "Repeated" drillType />

    <@getType drillType label />

    <@set drillType accessorType label "" "Repeated" "setScalar" />
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

<@build vv.types "Nullable" "Writer" />

<@build vv.types "Repeated" "Writer" />
}
