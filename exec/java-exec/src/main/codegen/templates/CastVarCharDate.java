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

<#list cast.types as type>
<#if type.major == "VarCharDate">  <#-- Template to convert from VarChar to Date, Time, TimeStamp, TimeStampTZ -->

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gcast/Cast${type.from}To${type.to}.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl.gcast;

import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;
import org.joda.time.MutableDateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.DateMidnight;
import org.apache.drill.exec.expr.fn.impl.DateUtility;

@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${type.to?upper_case}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
public class Cast${type.from}To${type.to} implements DrillSimpleFunc {

  @Param ${type.from}Holder in;
  @Output ${type.to}Holder out;

  public void setup(RecordBatch incoming) {
  }

  public void eval() {

      byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      String input = new String(buf);

      <#if type.to == "Date">
      org.joda.time.format.DateTimeFormatter f = org.apache.drill.exec.expr.fn.impl.DateUtility.getDateTimeFormatter();
      out.value = (org.joda.time.DateMidnight.parse(input, f).withZoneRetainFields(org.joda.time.DateTimeZone.UTC)).getMillis();

      <#elseif type.to == "TimeStamp">
      org.joda.time.format.DateTimeFormatter f = org.apache.drill.exec.expr.fn.impl.DateUtility.getDateTimeFormatter();
      out.value = org.joda.time.DateTime.parse(input, f).withZoneRetainFields(org.joda.time.DateTimeZone.UTC).getMillis();

      <#elseif type.to == "TimeStampTZ">
      org.joda.time.format.DateTimeFormatter f = org.apache.drill.exec.expr.fn.impl.DateUtility.getDateTimeFormatter();
      org.joda.time.DateTime dt = org.joda.time.DateTime.parse(input, f);
      out.value = dt.getMillis();
      out.index = org.apache.drill.exec.expr.fn.impl.DateUtility.getIndex(dt.getZone().toString());

      <#elseif type.to == "Time">
      org.joda.time.format.DateTimeFormatter f = org.apache.drill.exec.expr.fn.impl.DateUtility.getTimeFormatter();
      out.value = (int) (org.joda.time.DateMidnight.parse(input, f).withZoneRetainFields(org.joda.time.DateTimeZone.UTC)).getMillis();
      </#if>

  }
}
</#if> <#-- type.major -->
</#list>
