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
<@pp.changeOutputFile name="org/apache/drill/exec/store/JSONOutputRecordWriter.java" />
<#include "/@includes/license.ftl" />

package org.apache.drill.exec.store;

import com.fasterxml.jackson.core.JsonGenerator;
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
public abstract class JSONOutputRecordWriter extends AbstractRecordWriter implements RecordWriter {

  protected JsonGenerator gen;

<#list vv.types as type>
  <#list type.minor as minor>
    <#list vv.modes as mode>
  <#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />
  @Override
  public FieldConverter getNew${mode.prefix}${minor.class}Converter(int fieldId, String fieldName, FieldReader reader) {
    return new ${mode.prefix}${minor.class}JsonConverter(fieldId, fieldName, reader);
  }

  public class ${mode.prefix}${minor.class}JsonConverter extends FieldConverter {

    public ${mode.prefix}${minor.class}JsonConverter(int fieldId, String fieldName, FieldReader reader) {
      super(fieldId, fieldName, reader);
    }

    @Override
    public void startField() throws IOException {
      gen.writeFieldName(fieldName);
    }

    @Override
    public void writeField() throws IOException {
  <#if mode.prefix == "Nullable" >
    if (!reader.isSet()) {
      gen.writeNull();
      return;
    }
  <#elseif mode.prefix == "Repeated" >
    // empty lists are represented by simply not starting a field, rather than starting one and putting in 0 elements
    if (reader.size() == 0) {
      return;
    }
    gen.writeStartArray();
    for (int i = 0; i < reader.size(); i++) {
  <#else>
  </#if>

  <#if  minor.class == "TinyInt" ||
        minor.class == "UInt1" ||
        minor.class == "UInt2" ||
        minor.class == "SmallInt" ||
        minor.class == "Int" ||
        minor.class == "Decimal9" ||
        minor.class == "Float4" ||
        minor.class == "BigInt" ||
        minor.class == "Decimal18" ||
        minor.class == "UInt8" ||
        minor.class == "UInt4" ||
        minor.class == "Float8" ||
        minor.class == "Decimal28Sparse" ||
        minor.class == "Decimal28Dense" ||
        minor.class == "Decimal38Dense" ||
        minor.class == "Decimal38Sparse">
    <#if mode.prefix == "Repeated" >
      gen.writeNumber(reader.read${friendlyType}(i));
    <#else>
      gen.writeNumber(reader.read${friendlyType}());
    </#if>
  <#elseif minor.class == "Date" ||
              minor.class == "Time" ||
              minor.class == "TimeStamp" ||
              minor.class == "TimeTZ" ||
              minor.class == "TimeStampTZ" ||
              minor.class == "IntervalDay" ||
              minor.class == "Interval" ||
              minor.class == "VarChar" ||
              minor.class == "Var16Char" ||
              minor.class == "IntervalYear">
    <#if mode.prefix == "Repeated" >
              gen.writeString(reader.read${friendlyType}(i).toString());
    <#else>
      gen.writeString(reader.read${friendlyType}().toString());
    </#if>
  <#elseif
        minor.class == "Bit">
      <#if mode.prefix == "Repeated" >
              gen.writeBoolean(reader.read${friendlyType}(i));
      <#else>
      gen.writeBoolean(reader.read${friendlyType}());
      </#if>
  <#elseif
            minor.class == "VarBinary">
      <#if mode.prefix == "Repeated" >
              gen.writeBinary(reader.readByteArray(i));
      <#else>
      gen.writeBinary(reader.readByteArray());
      </#if>
  </#if>
  <#if mode.prefix == "Repeated">
    }
      gen.writeEndArray();
  </#if>
    }
  }
    </#list>
  </#list>
</#list>

}
