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
<#list vv.modes as mode>
<#list vv.types as type>
<#list type.minor as minor>

<#assign className="${mode.prefix}${minor.class}Holder" />
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/holders/${className}.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.holders;

<#include "/@includes/vv_imports.ftl" />

public final class ${className} implements ValueHolder{
  
  public static final MajorType TYPE = Types.${mode.name?lower_case}(MinorType.${minor.class?upper_case});
  
  public MajorType getType() {return TYPE;}
  
    <#if mode.name != "Repeated">
      
    public static final int WIDTH = ${type.width};
      <#if mode.name == "Optional">
      /** Whether the given holder holds a valid value.  1 means non-null.  0 means null. **/
      public int isSet;
      
      public boolean isSet() {
        return isSet == 1;
      }
      <#else>
    
      public boolean isSet() {
        return true;
      }
      
      </#if>
     
  
      
      <#if type.major != "VarLen">

      <#if (minor.class == "TimeStampTZ")>
      public long value;
      public int index;
      <#elseif (minor.class == "Interval")>
      public int months;
      public int days;
      public int milliSeconds;
      <#elseif (minor.class == "IntervalDay")>
      public int days;
      public int milliSeconds;
    <#elseif minor.class.startsWith("Decimal")>
    public int scale;
    public int precision;
    public static final int maxPrecision = ${minor.maxPrecisionDigits};
    <#if minor.class.startsWith("Decimal28") || minor.class.startsWith("Decimal38")>
    public int start;
    public ByteBuf buffer;
    public static final int nDecimalDigits = ${minor.nDecimalDigits};


    public int getInteger(int index) {
        int value = buffer.getInt(start + (index * 4));

        if (index == 0) {
            /* the first byte contains sign bit, return value without it */
            <#if minor.class.endsWith("Sparse")>
            value = (value & 0x7FFFFFFF);
            <#elseif minor.class.endsWith("Dense")>
            value = (value & 0x0000007F);
            </#if>
        }
        return value;
    }

    public void setInteger(int index, int value) {
        buffer.setInt(start + (index * 4), value);
    }

    public void setSign(boolean sign) {
      // Set MSB to 1 if sign is negative
      if (sign == true) {
        int value = getInteger(0);
        setInteger(0, (value | 0x80000000));
      }
    }

    public boolean getSign() {
      return ((buffer.getInt(start) & 0x80000000) != 0);
    }

    <#else>
    public ${minor.javaType!type.javaType} value;
    </#if>

      <#elseif (type.width > 8)>
      public int start;
      public ByteBuf buffer;
      <#else>
        public ${minor.javaType!type.javaType} value;
      </#if>
      <#else>
      /** The first offset (inclusive) into the buffer. **/
      public int start;
      
  /** The last offset (exclusive) into the buffer. **/
      public int end;
      
      /** The buffer holding actual values. **/
      public ByteBuf buffer;

      public String toString() {
      <#if mode.name == "Optional">
        if (isSet == 0)
          return "<NULL>";
      </#if>
        byte[] buf = new byte[end-start];
        buffer.getBytes(start, buf, 0, end-start);

        <#switch minor.class>
        <#case "Var16Char">
        return new String(buf, com.google.common.base.Charsets.UTF_16);
        <#break>
        <#case "VarChar">
        <#default>
        return new String(buf, com.google.common.base.Charsets.UTF_8);
        </#switch>
      }

      </#if>

    <#else> 
    
      /** The first index (inclusive) into the Vector. **/
      public int start;
      
      /** The last index (exclusive) into the Vector. **/
      public int end;
      
      /** The Vector holding the actual values. **/
      public ${minor.class}Vector vector;
      
      public boolean isSet() {
        return true;
      }

    </#if>
}

</#list>
</#list>
</#list>