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

    <#if mode.name != "Repeated">
      
    public static final int WIDTH = ${type.width};
      <#if mode.name == "Optional">
      /** Whether the given holder holds a valid value.  1 means non-null.  0 means null. **/
      public int isSet;
      </#if>
      
      <#if type.major != "VarLen">
      
      <#if (type.width > 8)>
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
        return new String(buf);
      }

      </#if>

    <#else> 
    
      /** The first index (inclusive) into the Vector. **/
      public int start;
      
      /** The last index (exclusive) into the Vector. **/
      public int end;
      
      /** The Vector holding the actual values. **/
      public ${minor.class}Vector vector;
    </#if>
  
    
}

</#list>
</#list>
</#list>