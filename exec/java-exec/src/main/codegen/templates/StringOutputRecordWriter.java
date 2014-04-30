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
<@pp.changeOutputFile name="org/apache/drill/exec/store/StringOutputRecordWriter.java" />
<#include "/@includes/license.ftl" />

package org.apache.drill.exec.store;

import com.google.common.collect.Lists;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.vector.*;

import java.io.IOException;
import java.lang.UnsupportedOperationException;
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
public abstract class StringOutputRecordWriter implements RecordWriter {

  private ValueVector[] columnVectors;

  public void updateSchema(BatchSchema schema) throws IOException {
    columnVectors = new ValueVector[schema.getFieldCount()];

    List<String> columnNames = Lists.newArrayList();
    for (int i=0; i<columnVectors.length; i++) {
      columnNames.add(schema.getColumn(i).getAsSchemaPath().getAsUnescapedPath());
    }

    startNewSchema(columnNames);

    for (int i=0; i<columnVectors.length; i++) {
      columnVectors[i] = TypeHelper.getNewVector(schema.getColumn(i), new TopLevelAllocator());
      AllocationHelper.allocate(columnVectors[i], 1, TypeHelper.getSize(schema.getColumn(i).getType()));
    }
  }

<#list vv.types as type>
  <#list type.minor as minor>
    <#list vv.modes as mode>
  @Override
  public void add${mode.prefix}${minor.class}Holder(int fieldId, ${mode.prefix}${minor.class}Holder valueHolder) throws IOException {
  <#if mode.prefix == "Nullable" >
    if (valueHolder.isSet == 0) {
      addField(fieldId, null);
      return;
    }
  <#elseif mode.prefix == "Repeated" >
    throw new UnsupportedOperationException("Repeated types are not supported.");
  }
    <#break>
  </#if>

  <#if  minor.class == "TinyInt" ||
        minor.class == "UInt1" ||
        minor.class == "UInt2" ||
        minor.class == "SmallInt" ||
        minor.class == "Int" ||
        minor.class == "UInt4" ||
        minor.class == "Float4" ||
        minor.class == "BigInt" ||
        minor.class == "UInt8" ||
        minor.class == "Float8">
    addField(fieldId, String.valueOf(valueHolder.value));
  <#elseif minor.class == "Bit">
    addField(fieldId, valueHolder.value == 0 ? "false" : "true");
  <#elseif
        minor.class == "Date" ||
        minor.class == "Time" ||
        minor.class == "TimeTZ" ||
        minor.class == "TimeStamp" ||
        minor.class == "TimeStampTZ" ||
        minor.class == "IntervalYear" ||
        minor.class == "IntervalDay" ||
        minor.class == "Interval" ||
        minor.class == "Decimal9" ||
        minor.class == "Decimal18" ||
        minor.class == "Decimal28Dense" ||
        minor.class == "Decimal38Dense" ||
        minor.class == "Decimal28Sparse" ||
        minor.class == "Decimal38Sparse">

    // TODO: error check
    ((${mode.prefix}${minor.class}Vector)columnVectors[fieldId]).getMutator().setSafe(0, valueHolder);
    Object obj = ((${mode.prefix}${minor.class}Vector)columnVectors[fieldId]).getAccessor().getObject(0);
    addField(fieldId, obj.toString());

  <#elseif minor.class == "VarChar" || minor.class == "Var16Char" || minor.class == "VarBinary">
    addField(fieldId, valueHolder.toString());
  </#if>
  }
    </#list>
  </#list>
</#list>

  public void cleanup() throws IOException {
    if (columnVectors != null){
      for(ValueVector vector : columnVectors)
        if (vector != null) vector.clear();
    }
  }

  public abstract void startNewSchema(List<String> columnNames) throws IOException;
  public abstract void addField(int fieldId, String value) throws IOException;
}
