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
<#macro bindReader prefix drillType>
  <#if drillType = "Decimal9" || drillType == "Decimal18">
    private MaterializedField field;
  </#if>
    private ${prefix}${drillType}Vector.Accessor accessor;

    @Override
    public void bind(ColumnReaderIndex vectorIndex, ValueVector vector) {
      bind(vectorIndex);
  <#if drillType = "Decimal9" || drillType == "Decimal18">
      field = vector.getField();
  </#if>
      accessor = ((${prefix}${drillType}Vector) vector).getAccessor();
    }

  <#if drillType = "Decimal9" || drillType == "Decimal18">
    @Override
    public void bind(ColumnReaderIndex vectorIndex, MaterializedField field, VectorAccessor va) {
      bind(vectorIndex, field, va);
      this.field = field;
    }

 </#if>
   private ${prefix}${drillType}Vector.Accessor accessor() {
      if (vectorAccessor == null) {
        return accessor;
      } else {
        return ((${prefix}${drillType}Vector) vectorAccessor.vector()).getAccessor();
      }
    }
</#macro>
<#macro get drillType accessorType label isArray>
    @Override
    public ${accessorType} get${label}(<#if isArray>int index</#if>) {
  <#if isArray>
    <#assign indexVar = "index"/>
    <#assign index = ", " + indexVar/>
    <#assign getObject = "getSingleObject"/>
  <#else>
    <#assign indexVar = ""/>
    <#assign index = ""/>
    <#assign getObject =" getObject"/>
  </#if>
  <#if drillType == "VarChar" || drillType == "Var16Char" || drillType == "VarBinary">
      return accessor().get(vectorIndex.vectorIndex()${index});
  <#elseif drillType == "Decimal9" || drillType == "Decimal18">
      return DecimalUtility.getBigDecimalFromPrimitiveTypes(
                accessor().get(vectorIndex.vectorIndex()${index}),
                field.getScale(),
                field.getPrecision());
  <#elseif accessorType == "BigDecimal" || accessorType == "Period">
      return accessor().${getObject}(vectorIndex.vectorIndex()${index});
  <#else>
      return accessor().get(vectorIndex.vectorIndex()${index});
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
<#macro bindWriter prefix drillType>
  <#if drillType = "Decimal9" || drillType == "Decimal18">
    private MaterializedField field;
  </#if>
    private ${prefix}${drillType}Vector.Mutator mutator;

    @Override
    public void bind(ColumnWriterIndex vectorIndex, ValueVector vector) {
      bind(vectorIndex);
  <#if drillType = "Decimal9" || drillType == "Decimal18">
      field = vector.getField();
  </#if>
      this.mutator = ((${prefix}${drillType}Vector) vector).getMutator();
    }
</#macro>
<#-- DRILL-5529 describes how required and repeated vectors don't implement
     fillEmpties() to recover from skiped writes. Since the mutator does
     not do the work, we must insert code here to do it.
     DRILL-5530 describes why required vectors may not be initialized
     to zeros. -->
<#macro fillEmpties drillType mode>
  <#if mode == "Repeated"  || (mode == "" && drillType != "Bit")>
        // Work-around for DRILL-5529: lack of fillEmpties for some vectors.
    <#if mode == "">
        // See DRILL-5530 for why this is needed for required vectors.
    </#if>
        if (lastWriteIndex + 1 < writeIndex) {
          mutator.fillEmptiesBounded(lastWriteIndex, writeIndex);
        }
 </#if>
</#macro>
<#macro set drillType accessorType label mode setFn>
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
              field.getScale(), field.getPrecision()));
  <#elseif drillType == "Decimal18">
        mutator.${setFn}(writeIndex,
            DecimalUtility.getDecimal18FromBigDecimal(value,
              field.getScale(), field.getPrecision()));
  <#elseif drillType == "IntervalYear">
        mutator.${setFn}(writeIndex, value.getYears() * 12 + value.getMonths());
  <#elseif drillType == "IntervalDay">
        mutator.${setFn}(writeIndex,<#if mode == "Nullable"> 1,</#if>
                      value.getDays(),
                      ((value.getHours() * 60 + value.getMinutes()) * 60 +
                       value.getSeconds()) * 1000 + value.getMillis());
  <#elseif drillType == "Interval">
        mutator.${setFn}(writeIndex,<#if mode == "Nullable"> 1,</#if>
                      value.getYears() * 12 + value.getMonths(),
                      value.getDays(),
                      ((value.getHours() * 60 + value.getMinutes()) * 60 +
                       value.getSeconds()) * 1000 + value.getMillis());
  <#else>
        mutator.${setFn}(writeIndex, <#if cast=="set">(${javaType}) </#if>value);
  </#if>
        lastWriteIndex = writeIndex;
      } catch (VectorOverflowException e) {
        vectorIndex.overflowed();
        throw e;
      }
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
    public void finishBatch() throws VectorOverflowException {
      final int rowCount = vectorIndex.vectorIndex();
  <#-- See note above for the fillEmpties macro. -->
  <#if mode == "Repeated"  || (mode == "" && drillType != "Bit")>
      // Work-around for DRILL-5529: lack of fillEmpties for some vectors.
    <#if mode == "">
      // See DRILL-5530 for why this is needed for required vectors.
    </#if>
      mutator.fillEmptiesBounded(lastWriteIndex, rowCount - 1);
   </#if>
      mutator.setValueCount(rowCount);
    }
</#macro>

package org.apache.drill.exec.vector.accessor;

import java.math.BigDecimal;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnReader;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnWriter;
import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;
import org.apache.drill.exec.vector.accessor.impl.AbstractArrayReader;
import org.apache.drill.exec.vector.accessor.impl.AbstractArrayWriter;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnReader.VectorAccessor;

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

  public static class ${drillType}ColumnReader extends AbstractColumnReader {

    <@bindReader "" drillType />

    <@getType drillType label />

    <@get drillType accessorType label false/>
  }

  public static class Nullable${drillType}ColumnReader extends AbstractColumnReader {

    <@bindReader "Nullable" drillType />

    <@getType drillType label />

    @Override
    public boolean isNull() {
      return accessor().isNull(vectorIndex.vectorIndex());
    }

    <@get drillType accessorType label false/>
  }

  public static class Repeated${drillType}ColumnReader extends AbstractArrayReader {

    <@bindReader "Repeated" drillType />

    <@getType drillType label />

    @Override
    public int size() {
      return accessor().getInnerValueCountAt(vectorIndex.vectorIndex());
    }

    <@get drillType accessorType label true/>
  }

  public static class ${drillType}ColumnWriter extends AbstractColumnWriter {

    <@bindWriter "" drillType />

    <@getType drillType label />

    <@set drillType accessorType label "" "setScalar" />

    <@finishBatch drillType "" />
  }

  public static class Nullable${drillType}ColumnWriter extends AbstractColumnWriter {

    <@bindWriter "Nullable" drillType />

    <@getType drillType label />

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

    <@set drillType accessorType label "Nullable" "setScalar" />

    <@finishBatch drillType "Nullable" />
  }

  public static class Repeated${drillType}ColumnWriter extends AbstractArrayWriter {

    <@bindWriter "Repeated" drillType />

    <@getType drillType label />

    protected BaseRepeatedValueVector.BaseRepeatedMutator mutator() {
      return mutator;
    }

    <@set drillType accessorType label "Repeated" "addEntry" />

    <@finishBatch drillType "Repeated" />
  }

    </#if>
  </#list>
</#list>
  public static void defineReaders(
      Class<? extends AbstractColumnReader> readers[][]) {
<#list vv.types as type>
  <#list type.minor as minor>
    <#assign drillType=minor.class>
    <#assign notyet=minor.accessorDisabled!type.accessorDisabled!false>
    <#if ! notyet>
    <#assign typeEnum=drillType?upper_case>
    readers[MinorType.${typeEnum}.ordinal()][DataMode.REQUIRED.ordinal()] = ${drillType}ColumnReader.class;
    readers[MinorType.${typeEnum}.ordinal()][DataMode.OPTIONAL.ordinal()] = Nullable${drillType}ColumnReader.class;
    </#if>
  </#list>
</#list>
  }

  public static void defineWriters(
      Class<? extends AbstractColumnWriter> writers[][]) {
<#list vv.types as type>
  <#list type.minor as minor>
    <#assign drillType=minor.class>
    <#assign notyet=minor.accessorDisabled!type.accessorDisabled!false>
    <#if ! notyet>
    <#assign typeEnum=drillType?upper_case>
    writers[MinorType.${typeEnum}.ordinal()][DataMode.REQUIRED.ordinal()] = ${drillType}ColumnWriter.class;
    writers[MinorType.${typeEnum}.ordinal()][DataMode.OPTIONAL.ordinal()] = Nullable${drillType}ColumnWriter.class;
    </#if>
  </#list>
</#list>
  }

  public static void defineArrayReaders(
      Class<? extends AbstractArrayReader> readers[]) {
<#list vv.types as type>
  <#list type.minor as minor>
    <#assign drillType=minor.class>
    <#assign notyet=minor.accessorDisabled!type.accessorDisabled!false>
    <#if ! notyet>
    <#assign typeEnum=drillType?upper_case>
    readers[MinorType.${typeEnum}.ordinal()] = Repeated${drillType}ColumnReader.class;
    </#if>
  </#list>
</#list>
  }

  public static void defineArrayWriters(
      Class<? extends AbstractArrayWriter> writers[]) {
<#list vv.types as type>
  <#list type.minor as minor>
    <#assign drillType=minor.class>
    <#assign notyet=minor.accessorDisabled!type.accessorDisabled!false>
    <#if ! notyet>
    <#assign typeEnum=drillType?upper_case>
    writers[MinorType.${typeEnum}.ordinal()] = Repeated${drillType}ColumnWriter.class;
    </#if>
  </#list>
</#list>
  }
}
