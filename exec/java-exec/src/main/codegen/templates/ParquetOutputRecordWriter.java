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

import org.joda.time.DateTimeUtils;
import parquet.io.api.Binary;

import java.lang.Override;
import java.lang.RuntimeException;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="org/apache/drill/exec/store/ParquetOutputRecordWriter.java" />
<#include "/@includes/license.ftl" />

package org.apache.drill.exec.store;

import com.google.common.collect.Lists;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.store.EventBasedRecordWriter.FieldConverter;
import org.apache.drill.exec.store.parquet.ParquetTypeHelper;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.io.api.Binary;
import io.netty.buffer.ByteBuf;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;


import org.apache.drill.common.types.TypeProtos;

import org.joda.time.DateTimeUtils;

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
public abstract class ParquetOutputRecordWriter extends AbstractRecordWriter implements RecordWriter {

  private RecordConsumer consumer;
  private MessageType schema;
  public static final long JULIAN_DAY_EPOC = DateTimeUtils.toJulianDayNumber(0);

  public void setUp(MessageType schema, RecordConsumer consumer) {
    this.schema = schema;
    this.consumer = consumer;
  }

<#list vv.types as type>
  <#list type.minor as minor>
    <#list vv.modes as mode>
  @Override
  public FieldConverter getNew${mode.prefix}${minor.class}Converter(int fieldId, String fieldName, FieldReader reader) {
    return new ${mode.prefix}${minor.class}ParquetConverter(fieldId, fieldName, reader);
  }

  public class ${mode.prefix}${minor.class}ParquetConverter extends FieldConverter {
    private Nullable${minor.class}Holder holder = new Nullable${minor.class}Holder();

    public ${mode.prefix}${minor.class}ParquetConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void writeField() throws IOException {
  <#if mode.prefix == "Nullable" >
    if (!reader.isSet()) {
      return;
    }
  <#elseif mode.prefix == "Repeated" >
    // empty lists are represented by simply not starting a field, rather than starting one and putting in 0 elements
    if (reader.size() == 0) {
      return;
    }
    consumer.startField(fieldName, fieldId);
    for (int i = 0; i < reader.size(); i++) {
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
            reader.read(i, holder);
            consumer.addInteger(holder.value);
    <#else>
    consumer.startField(fieldName, fieldId);
    reader.read(holder);
    consumer.addInteger(holder.value);
    consumer.endField(fieldName, fieldId);
    </#if>
  <#elseif
        minor.class == "Float4">
      <#if mode.prefix == "Repeated" >
              reader.read(i, holder);
              consumer.addFloat(holder.value);
      <#else>
    consumer.startField(fieldName, fieldId);
    reader.read(holder);
    consumer.addFloat(holder.value);
    consumer.endField(fieldName, fieldId);
      </#if>
  <#elseif
        minor.class == "BigInt" ||
        minor.class == "Decimal18" ||
        minor.class == "TimeStamp" ||
        minor.class == "UInt8">
      <#if mode.prefix == "Repeated" >
              reader.read(i, holder);
              consumer.addLong(holder.value);
      <#else>
    consumer.startField(fieldName, fieldId);
    reader.read(holder);
    consumer.addLong(holder.value);
    consumer.endField(fieldName, fieldId);
      </#if>
  <#elseif minor.class == "Date">
    <#if mode.prefix == "Repeated" >
      reader.read(i, holder);
      consumer.addInteger((int) (DateTimeUtils.toJulianDayNumber(holder.value) + JULIAN_DAY_EPOC));
    <#else>
      consumer.startField(fieldName, fieldId);
      reader.read(holder);
      // convert from internal Drill date format to Julian Day centered around Unix Epoc
      consumer.addInteger((int) (DateTimeUtils.toJulianDayNumber(holder.value) + JULIAN_DAY_EPOC));
      consumer.endField(fieldName, fieldId);
    </#if>
  <#elseif
        minor.class == "Float8">
      <#if mode.prefix == "Repeated" >
              reader.read(i, holder);
              consumer.addDouble(holder.value);
      <#else>
    consumer.startField(fieldName, fieldId);
    reader.read(holder);
    consumer.addDouble(holder.value);
    consumer.endField(fieldName, fieldId);
      </#if>
  <#elseif
        minor.class == "Bit">
      <#if mode.prefix == "Repeated" >
              reader.read(i, holder);
              consumer.addBoolean(holder.value == 1);
      <#else>
    consumer.startField(fieldName, fieldId);
    consumer.addBoolean(holder.value == 1);
    consumer.endField(fieldName, fieldId);
      </#if>
  <#elseif
        minor.class == "Decimal28Sparse" ||
        minor.class == "Decimal38Sparse">
      <#if mode.prefix == "Repeated" >
      <#else>
      consumer.startField(fieldName, fieldId);
      reader.read(holder);
      byte[] bytes = DecimalUtility.getBigDecimalFromSparse(
              holder.buffer, holder.start, ${minor.class}Holder.nDecimalDigits, holder.scale).unscaledValue().toByteArray();
      byte[] output = new byte[ParquetTypeHelper.getLengthForMinorType(MinorType.${minor.class?upper_case})];
      if (holder.getSign(holder.start, holder.buffer)) {
        Arrays.fill(output, 0, output.length - bytes.length, (byte)0xFF);
      } else {
        Arrays.fill(output, 0, output.length - bytes.length, (byte)0x0);
      }
      System.arraycopy(bytes, 0, output, output.length - bytes.length, bytes.length);
      consumer.addBinary(Binary.fromByteArray(output));
      consumer.endField(fieldName, fieldId);
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
      reader.read(i, holder);
      //consumer.startField(fieldName, fieldId);
      consumer.addBinary(Binary.fromByteBuffer(holder.buffer.nioBuffer(holder.start, holder.end - holder.start)));
      //consumer.endField(fieldName, fieldId);
    <#else>
    reader.read(holder);
    ByteBuf buf = holder.buffer;
    consumer.startField(fieldName, fieldId);
    consumer.addBinary(Binary.fromByteBuffer(holder.buffer.nioBuffer(holder.start, holder.end - holder.start)));
    consumer.endField(fieldName, fieldId);
    </#if>
  </#if>
  <#if mode.prefix == "Repeated">
    }
    consumer.endField(fieldName, fieldId);
  </#if>
    }
  }
    </#list>
  </#list>
</#list>

}
