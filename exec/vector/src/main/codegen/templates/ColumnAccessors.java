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
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.accessor.reader.BaseScalarReader;
import org.apache.drill.exec.vector.accessor.reader.BaseElementReader;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor;
import org.apache.drill.exec.vector.accessor.writer.BaseScalarWriter;
import org.apache.drill.exec.vector.accessor.writer.OffsetVectorWriter;

import com.google.common.base.Charsets;

import io.netty.buffer.DrillBuf;
import io.netty.util.internal.PlatformDependent;

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

  public static final int MIN_BUFFER_SIZE = 4096;

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
      <#assign varWidth = drillType == "VarChar" || drillType == "Var16Char" || drillType == "VarBinary" />
      <#if drillType = "Decimal9" || drillType == "Decimal18" ||
           drillType == "Decimal28Sparse" || drillType == "Decimal38Sparse">
    private MajorType type;
      </#if>
      <#if varWidth>
    private OffsetVectorWriter offsetsWriter = new OffsetVectorWriter();
      <#else>
    private static final int VALUE_WIDTH = ${drillType}Vector.VALUE_WIDTH;
      </#if>
    private ${drillType}Vector vector;

    @Override
    public final void bindVector(final ValueVector vector) {
      <#if drillType = "Decimal9" || drillType == "Decimal18" ||
           drillType == "Decimal28Sparse" || drillType == "Decimal38Sparse">
      type = vector.getField().getType();
      </#if>
      this.vector = (${drillType}Vector) vector;
      setAddr(this.vector.getBuffer());
      <#if drillType == "VarChar" || drillType == "Var16Char" || drillType == "VarBinary">
      offsetsWriter.bindVector(this.vector.getOffsetVector());
      <#-- lastWriteIndex unused for variable width vectors. -->
      <#else>
      lastWriteIndex = -1;
      </#if>
    }

     <#-- All change of buffer comes through this function to allow capturing
          the buffer address and capacity. Only two ways to set the buffer:
          by binding to a vector in bindVector(), or by resizing the vector
          in writeIndex(). -->
    private final void setAddr(final DrillBuf buf) {
      bufAddr = buf.addr();
      <#if varWidth>
      capacity = buf.capacity();
      <#else>
      <#-- Turns out that keeping track of capacity as the count of
           values simplifies the per-value code path. -->
      capacity = buf.capacity() / VALUE_WIDTH;
      </#if>
    }

      <#if varWidth>
    @Override
    public void bindIndex(final ColumnWriterIndex index) {
      offsetsWriter.bindIndex(index);
      super.bindIndex(index);
    }

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
    <#-- This is performance critical code; every operation counts.
         Please thoughtful when changing the code.
         Generated per class in the belief that the JVM will optimize the
         code path for each value width. Also, the reallocRaw() and
         setFoo() methods are type specific. (reallocRaw() could be virtual,
         but the PlatformDependent.setFoo() cannot be.
         This is a bit tricky. This method has side effects, by design.
         The current vector buffer, and buffer address, will change in
         this method when a vector grows or overflows. So, don't use this
         method in inline calls of the form
         vector.getBuffer().doSomething(writeIndex());
         The buffer obtained by getBuffer() can be different than the current
         buffer after writeIndex().
         -->
      <#if varWidth>
    private final int writeIndex(final int width) {
      int writeOffset = offsetsWriter.writeOffset();
      if (writeOffset + width < capacity) {
        return writeOffset;
      }
      <#else>
    private final int writeIndex() {
      <#-- "Fast path" for the normal case of no fills, no overflow.
            This is the only bounds check we want to do for the entire
            set operation. -->
      int writeIndex = vectorIndex.vectorIndex();
      if (lastWriteIndex + 1 == writeIndex && writeIndex < capacity) {
        lastWriteIndex = writeIndex;
        return writeIndex;
      }
      </#if>
      <#-- Either empties must be filed or the vector is full. -->
      <#if varWidth>
      int size = writeOffset + width;
      if (size > capacity) {
      <#else>
      if (writeIndex >= capacity) {
        int size = (writeIndex + 1) * VALUE_WIDTH;
      </#if>
        <#-- Two cases: grow this vector or allocate a new one. -->
        if (size > ValueVector.MAX_BUFFER_SIZE) {
          <#-- Allocate a new vector, or throw an exception if overflow is not supported.
               If overflow is supported, the callback will call finish(), which will
               fill empties, so no need to do that here. The call to finish() will
               also set the final writer index for the current vector. Then, bindVector() will
               be called to provide the new vector. The write index changes with
               the new vector. -->
          vectorIndex.overflowed();
      <#if varWidth>
          writeOffset = offsetsWriter.writeOffset();
      <#else>
          writeIndex = vectorIndex.vectorIndex();
      </#if>
        } else {
          <#-- Optimized form of reAlloc() which does not zero memory, does not do bounds
               checks (since they were already done above) and which returns
               the new buffer to save a method call. The write index and offset
               remain unchanged. Since some vectors start off as 0 length, set a
               minimum size to avoid silly thrashing on early rows. -->
          if (size < MIN_BUFFER_SIZE) {
            size = MIN_BUFFER_SIZE;
          }
          setAddr(vector.reallocRaw(BaseAllocator.nextPowerOfTwo(size)));
        }
      }
      <#-- Fill empties. This is required because the allocated memory is not
           zero-filled. -->
      <#if ! varWidth>
      while (lastWriteIndex < writeIndex - 1) {
        <#assign putAddr = "bufAddr + ++lastWriteIndex * VALUE_WIDTH" />
        <#if drillType == "Decimal9">
        PlatformDependent.putInt(${putAddr}, 0);
        <#elseif drillType == "Decimal18">
        PlatformDependent.putLong(${putAddr}, 0);
        <#elseif drillType == "Decimal28Sparse" || drillType == "Decimal38Sparse">
        long addr = ${putAddr};
        for (int i = 0; i < VALUE_WIDTH / 4; i++, addr += 4) {
          PlatformDependent.putInt(addr, 0);
        }
        <#elseif drillType == "IntervalYear">
        PlatformDependent.putInt(${putAddr}, 0);
        <#elseif drillType == "IntervalDay">
        final long addr = ${putAddr};
        PlatformDependent.putInt(addr,     0);
        PlatformDependent.putInt(addr + 4, 0);
        <#elseif drillType == "Interval">
        final long addr = ${putAddr};
        PlatformDependent.putInt(addr,     0);
        PlatformDependent.putInt(addr + 4, 0);
        PlatformDependent.putInt(addr + 8, 0);
        <#elseif drillType == "Float4">
        PlatformDependent.putInt(${putAddr}, 0);
        <#elseif drillType == "Float8">
        PlatformDependent.putLong(${putAddr}, 0);
        <#else>
        PlatformDependent.put${putType?cap_first}(${putAddr}, <#if doCast>(${putType}) </#if>0);
        </#if>
      }
      <#-- Track the last write location for zero-fill use next time around. -->
      lastWriteIndex = writeIndex;
      return writeIndex;
      <#else>
      return writeOffset;
      </#if>
    }

    @Override
    public final void set${label}(final ${accessorType} value${args}) {
      <#-- Must compute the write offset first; can't be inline because the
           writeOffset() function has a side effect of possibly changing the buffer
           address (bufAddr). -->
      <#if varWidth>
      final int offset = writeIndex(len);
      <#else>
      final int writeIndex = writeIndex();
      <#assign putAddr = "bufAddr + writeIndex * VALUE_WIDTH">
      </#if>
      <#if varWidth>
      PlatformDependent.copyMemory(value, 0, bufAddr + offset, len);
      offsetsWriter.setOffset(offset + len);
      <#elseif drillType == "Decimal9">
      PlatformDependent.putInt(${putAddr},
          DecimalUtility.getDecimal9FromBigDecimal(value,
                type.getScale(), type.getPrecision()));
      <#elseif drillType == "Decimal18">
      PlatformDependent.putLong(${putAddr},
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
      PlatformDependent.putInt(${putAddr},
                value.getYears() * 12 + value.getMonths());
      <#elseif drillType == "IntervalDay">
      final long addr = ${putAddr};
      PlatformDependent.putInt(addr,     value.getDays());
      PlatformDependent.putInt(addr + 4, periodToMillis(value));
      <#elseif drillType == "Interval">
      final long addr = ${putAddr};
      PlatformDependent.putInt(addr,     value.getYears() * 12 + value.getMonths());
      PlatformDependent.putInt(addr + 4, value.getDays());
      PlatformDependent.putInt(addr + 8, periodToMillis(value));
      <#elseif drillType == "Float4">
      PlatformDependent.putInt(${putAddr}, Float.floatToRawIntBits((float) value));
      <#elseif drillType == "Float8">
      PlatformDependent.putLong(${putAddr}, Double.doubleToRawLongBits(value));
      <#else>
      PlatformDependent.put${putType?cap_first}(${putAddr}, <#if doCast>(${putType}) </#if>value);
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

    @Override
    public final void finish() {
      <#if varWidth>
      vector.getBuffer().writerIndex(offsetsWriter.writeOffset());
      offsetsWriter.finish();
      <#else>
      <#-- Done this way to avoid another drill buf access in value set path.
           Though this calls writeOffset(), which handles vector overflow,
           such overflow should never occur because here we are simply
           finalizing a position already set. However, the vector size may
           grow and the "missing" values may be zero-filled. Note that, in
           odd cases, the call to writeOffset() might cause the vector to
           resize (as part of filling empties), so grab the buffer AFTER
           the call to writeOffset(). -->
      final int finalIndex = writeIndex(<#if varWidth>0</#if>);
      vector.getBuffer().writerIndex(finalIndex * VALUE_WIDTH);
      </#if>
    }
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
