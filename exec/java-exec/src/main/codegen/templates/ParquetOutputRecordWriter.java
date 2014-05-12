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

import parquet.io.api.Binary;

import java.lang.Override;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="org/apache/drill/exec/store/ParquetOutputRecordWriter.java" />
<#include "/@includes/license.ftl" />

package org.apache.drill.exec.store;

import com.google.common.collect.Lists;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.store.parquet.ParquetTypeHelper;
import org.apache.drill.exec.vector.*;
import org.apache.drill.common.util.DecimalUtility;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.io.api.Binary;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.lang.UnsupportedOperationException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Abstract implementation of RecordWriter interface which exposes interface:
 *    {@link #writeHeader(List)}
 *    {@link #addField(int,String)}
 * to output the data in string format instead of implementing addField for each type holder.
 *
 * This is useful for text format writers such as CSV, TSV etc.
 */
public abstract class ParquetOutputRecordWriter implements RecordWriter {

  private RecordConsumer consumer;
  private MessageType schema;

  public void setUp(MessageType schema, RecordConsumer consumer) {
    this.schema = schema;
    this.consumer = consumer;
  }

<#list vv.types as type>
  <#list type.minor as minor>
    <#list vv.modes as mode>
  @Override
  public void add${mode.prefix}${minor.class}Holder(int fieldId, ${mode.prefix}${minor.class}Holder valueHolder) throws IOException {
  <#if mode.prefix == "Nullable" >
    if (valueHolder.isSet == 0) {
      return;
    }
  <#elseif mode.prefix == "Repeated" >
    consumer.startField(schema.getFieldName(fieldId), fieldId);
    for (int i = valueHolder.start; i < valueHolder.end; i++) {
  </#if>

  <#if  minor.class == "TinyInt" ||
        minor.class == "UInt1" ||
        minor.class == "UInt2" ||
        minor.class == "SmallInt" ||
        minor.class == "Int" ||
        minor.class == "Time" ||
        minor.class == "IntervalYear" ||
        minor.class == "Decimal9" ||
        minor.class == "UInt4">
    <#if mode.prefix == "Repeated" >
            consumer.addInteger(valueHolder.vector.getAccessor().get(i));
    <#else>
    consumer.startField(schema.getFieldName(fieldId), fieldId);
    consumer.addInteger(valueHolder.value);
    consumer.endField(schema.getFieldName(fieldId), fieldId);
    </#if>
  <#elseif
        minor.class == "Float4">
      <#if mode.prefix == "Repeated" >
              consumer.addFloat(valueHolder.vector.getAccessor().get(i));
      <#else>
    consumer.startField(schema.getFieldName(fieldId), fieldId);
    consumer.addFloat(valueHolder.value);
    consumer.endField(schema.getFieldName(fieldId), fieldId);
      </#if>
  <#elseif
        minor.class == "BigInt" ||
        minor.class == "Decimal18" ||
        minor.class == "TimeStamp" ||
        minor.class == "Date" ||
        minor.class == "UInt8">
      <#if mode.prefix == "Repeated" >
              consumer.addLong(valueHolder.vector.getAccessor().get(i));
      <#else>
    consumer.startField(schema.getFieldName(fieldId), fieldId);
    consumer.addLong(valueHolder.value);
    consumer.endField(schema.getFieldName(fieldId), fieldId);
      </#if>
  <#elseif
        minor.class == "Float8">
      <#if mode.prefix == "Repeated" >
              consumer.addDouble(valueHolder.vector.getAccessor().get(i));
      <#else>
    consumer.startField(schema.getFieldName(fieldId), fieldId);
    consumer.addDouble(valueHolder.value);
    consumer.endField(schema.getFieldName(fieldId), fieldId);
      </#if>
  <#elseif
        minor.class == "Bit">
      <#if mode.prefix == "Repeated" >
              consumer.addBoolean(valueHolder.vector.getAccessor().get(i) == 1);
      <#else>
    consumer.startField(schema.getFieldName(fieldId), fieldId);
    consumer.addBoolean(valueHolder.value == 1);
    consumer.endField(schema.getFieldName(fieldId), fieldId);
      </#if>
  <#elseif
        minor.class == "Decimal28Sparse" ||
        minor.class == "Decimal38Sparse">
      <#if mode.prefix == "Repeated" >
      <#else>
      consumer.startField(schema.getFieldName(fieldId), fieldId);
      byte[] bytes = DecimalUtility.getBigDecimalFromSparse(
              valueHolder.buffer, valueHolder.start, ${minor.class}Holder.nDecimalDigits, valueHolder.scale).unscaledValue().toByteArray();
      byte[] output = new byte[ParquetTypeHelper.getLengthForMinorType(MinorType.${minor.class?upper_case})];
      if (valueHolder.sign) {
        Arrays.fill(output, 0, output.length - bytes.length, (byte)0xFF);
      } else {
        Arrays.fill(output, 0, output.length - bytes.length, (byte)0x0);
      }
      System.arraycopy(bytes, 0, output, output.length - bytes.length, bytes.length);
      consumer.addBinary(Binary.fromByteArray(output));
      consumer.endField(schema.getFieldName(fieldId), fieldId);
      </#if>
  <#elseif
        minor.class == "TimeTZ" ||
        minor.class == "TimeStampTZ" ||
        minor.class == "IntervalDay" ||
        minor.class == "Interval" ||
        minor.class == "Decimal28Dense" ||
        minor.class == "Decimal38Dense">

      <#if mode.prefix == "Repeated" >
      <#else>

      </#if>
  <#elseif minor.class == "VarChar" || minor.class == "Var16Char" || minor.class == "VarBinary">
    <#if mode.prefix == "Repeated">
      ${minor.class}Holder singleHolder = new ${minor.class}Holder();
      valueHolder.vector.getAccessor().get(i, singleHolder);
      consumer.startField(schema.getFieldName(fieldId), fieldId);
      consumer.addBinary(Binary.fromByteBuffer(singleHolder.buffer.nioBuffer(singleHolder.start, singleHolder.end - singleHolder.start)));
      consumer.endField(schema.getFieldName(fieldId), fieldId);
    <#else>
    ByteBuf buf = valueHolder.buffer;
    consumer.startField(schema.getFieldName(fieldId), fieldId);
    consumer.addBinary(Binary.fromByteBuffer(valueHolder.buffer.nioBuffer(valueHolder.start, valueHolder.end - valueHolder.start)));
    consumer.endField(schema.getFieldName(fieldId), fieldId);
    </#if>
  </#if>
  <#if mode.prefix == "Repeated">
    }
    consumer.endField(schema.getFieldName(fieldId), fieldId);
  </#if>
  }
    </#list>
  </#list>
</#list>

}
