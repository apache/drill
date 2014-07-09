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

import java.lang.Override;

<@pp.dropOutputFile />
<#list vv.types as type>
<#list type.minor as minor>
<#list ["", "Nullable"] as mode>
<#assign name = mode + minor.class?cap_first />
<#assign javaType = (minor.javaType!type.javaType) />
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/accessor/${name}Accessor.java" />
<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector.accessor;

<#include "/@includes/vv_imports.ftl" />

@SuppressWarnings("unused")
public class ${name}Accessor extends AbstractSqlAccessor{
  <#if mode == "Nullable">
  private static final MajorType TYPE = Types.optional(MinorType.${minor.class?upper_case});
  <#else>
  private static final MajorType TYPE = Types.required(MinorType.${minor.class?upper_case});
  </#if>
  
  private final ${name}Vector.Accessor ac;
  
  public ${name}Accessor(${name}Vector vector){
    this.ac = vector.getAccessor();
  }

  <#if minor.class != "TimeStampTZ" && minor.class != "TimeStamp" && minor.class != "Time" && minor.class != "Date">
  public Object getObject(int index){
    return ac.getObject(index);
  }
  </#if>
  
  <#if type.major == "VarLen">

  @Override 
  public InputStream getStream(int index){
    ${name}Holder h = new ${name}Holder();
    ac.get(index, h);
    return new ByteBufInputStream(h.buffer.slice(h.start, h.end));
  }
  
  @Override 
  public byte[] getBytes(int index){
    return ac.get(index);
  }
  
  <#switch minor.class>
    <#case "VarBinary">
    public String getString(int index) {
      byte [] b = ac.get(index);
      return DrillStringUtils.toBinaryString(b);
    }
      <#break>
    <#case "VarChar">
    @Override 
    public InputStreamReader getReader(int index){
      return new InputStreamReader(getStream(index), Charsets.UTF_8);
    }
    
    @Override 
    public String getString(int index){
      return new String(getBytes(index), Charsets.UTF_8);
    }
    
    
      <#break>
    <#case "Var16Char">
    @Override 
    public InputStreamReader getReader(int index){
      return new InputStreamReader(getStream(index), Charsets.UTF_16);
    }
    
    @Override 
    public String getString(int index){
      return new String(getBytes(index), Charsets.UTF_16);
    }
        
    
      <#break>
    <#default> 
    This is uncompilable code
  </#switch>

  <#else>
  <#if minor.class == "TimeStampTZ">
  public Object getObject(int index) {
    return getTimestamp(index);
  }

  @Override
  public Timestamp getTimestamp(int index) {
    return new Timestamp(ac.getObject(index).getMillis());
  }
  <#elseif minor.class == "Interval" || minor.class == "IntervalDay" || minor.class == "IntervalYear">
  @Override
  public String getString(int index) {
      return String.valueOf(ac.getAsStringBuilder(index));
  }
  <#elseif minor.class.startsWith("Decimal")>
  @Override
  public BigDecimal getBigDecimal(int index) {
      return ac.getObject(index);
  }
  <#elseif minor.class == "Date">
  public Object getObject(int index) {
    return getDate(index);
  }

  @Override
  public Date getDate(int index) {
    <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
    </#if>
    org.joda.time.DateTime date = new org.joda.time.DateTime(ac.get(index), org.joda.time.DateTimeZone.UTC);
    date = date.withZoneRetainFields(org.joda.time.DateTimeZone.getDefault());
    return new Date(date.getMillis());
  }
  <#elseif minor.class == "TimeStamp">
  public Object getObject(int index) {
    return getTimestamp(index);
  }

  @Override
  public Timestamp getTimestamp(int index) {
    <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
    </#if>
    org.joda.time.DateTime date = new org.joda.time.DateTime(ac.get(index), org.joda.time.DateTimeZone.UTC);
    date = date.withZoneRetainFields(org.joda.time.DateTimeZone.getDefault());
    return new Timestamp(date.getMillis());
  }
  <#elseif minor.class == "Time">
  public Object getObject(int index) {
    return getTime(index);
  }

  @Override
  public Time getTime(int index) {
    <#if mode == "Nullable">
    if (ac.isNull(index)) {
      return null;
    }
    </#if>
    org.joda.time.DateTime time = new org.joda.time.DateTime(ac.get(index), org.joda.time.DateTimeZone.UTC);
    time = time.withZoneRetainFields(org.joda.time.DateTimeZone.getDefault());
    return new TimePrintMillis(time.getMillis());
  }
  <#else>
  @Override
  public ${javaType} get${javaType?cap_first}(int index){
    return ac.get(index);
  }
  </#if>
  </#if>
  
  @Override
  public boolean isNull(int index){
    return false;
  }
  
  @Override
  MajorType getType(){return TYPE;};

}


</#list>
</#list>
</#list>