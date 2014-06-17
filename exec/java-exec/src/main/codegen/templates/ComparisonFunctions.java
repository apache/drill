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

<#macro compareBlock mode left right output nullCompare>

outside:{
  
  <#if nullCompare>
      <#if left?starts_with("Nullable")>
        <#if right?starts_with("Nullable")>
        <#-- Both are nullable. -->
        if(left.isSet == 0){
          if(right.isSet == 0){
            ${output} = 0;
            break outside;
          }else{
            ${output} = 1;
            break outside;
          }
        }else if(right.isSet == 0){
          ${output} = -1;
          break outside;
        }
        <#else>
        <#-- Left is nullable but right is not. -->
        if(left.isSet == 0){
          ${output} = 1;
          break outside;
        }
        </#if>
    <#elseif right?starts_with("Nullable")>
      if(right.isSet == 0){
        ${output} = -1;
        break outside;
      }
      </#if>
    </#if>
    
    <#if mode == "var">
    
    for (int l = left.start, r = right.start; l < left.end && r < right.end; l++, r++) {
      byte leftByte = left.buffer.getByte(l);
      byte rightByte = right.buffer.getByte(r);
      if (leftByte != rightByte) {
        ${output} = ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
        break outside;
      }
    }
    
    int l = (left.end - left.start) - (right.end - right.start);
    if (l > 0) {
      ${output} = 1;
      break outside;
    } else if (l == 0) {
      ${output} = 0;
      break outside;
    } else {
      ${output} = -1;
      break outside;
    }
    <#elseif mode == "fixed">
      ${output} = left.value < right.value ? -1 : ((left.value == right.value)? 0 : 1);
    </#if>    
  

  
  

}
</#macro>

<#list compare.types as type>
<#list type.comparables as left>
<#list type.comparables as right>
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/GCompare${left}${right}.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

@SuppressWarnings("unused")
public class GCompare${left}${right}{

  @FunctionTemplate(name = "compare_to", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Compare${left}${right} implements DrillSimpleFunc {

      @Param ${left}Holder left;
      @Param ${right}Holder right;
      @Output IntHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        <@compareBlock mode=type.mode left=left right=right output="out.value" nullCompare=true />
      }
  }
  
  @FunctionTemplate(names = {"less_than", "<"}, scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class LessThan${left}${right} implements DrillSimpleFunc {

      @Param ${left}Holder left;
      @Param ${right}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        sout: {
        <#if left?starts_with("Nullable")>
        if(left.isSet ==0){
          out.value = 0;
          break sout;
        }
        </#if>
        <#if right?starts_with("Nullable")>
        if(right.isSet ==0){
          out.value = 0;
          break sout;
        }
        </#if>

        <#if type.mode == "var" >
        int cmp;
        <@compareBlock mode=type.mode left=left right=right output="cmp" nullCompare=false/>
        out.value = cmp == -1 ? 1 : 0;
        <#else>
        out.value = left.value < right.value ? 1 : 0;
        </#if>

        }
      }
  }
  
  @FunctionTemplate(names = {"less_than_or_equal_to", "<="}, scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class LessThanE${left}${right} implements DrillSimpleFunc {

      @Param ${left}Holder left;
      @Param ${right}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        sout: {
        <#if left?starts_with("Nullable")>
        if(left.isSet ==0){
          out.value = 0;
          break sout;
        }
        </#if>
        <#if right?starts_with("Nullable")>
        if(right.isSet ==0){
          out.value = 0;
          break sout;
        }
        </#if>
        
        <#if type.mode == "var" >
        int cmp;
        <@compareBlock mode=type.mode left=left right=right output="cmp" nullCompare=false/>
        out.value = cmp < 1 ? 1 : 0;
        <#else>
        out.value = left.value <= right.value ? 1 : 0;
        </#if>

        }
    }
  }
  
  @FunctionTemplate(names = {"greater_than", ">"}, scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class GreaterThan${left}${right} implements DrillSimpleFunc {

      @Param ${left}Holder left;
      @Param ${right}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        sout: {
        <#if left?starts_with("Nullable")>
        if(left.isSet ==0){
          out.value = 0;
          break sout;
        }
        </#if>
        <#if right?starts_with("Nullable")>
        if(right.isSet ==0){
          out.value = 0;
          break sout;
        }
        </#if>
        
        <#if type.mode == "var" >
        int cmp;
        <@compareBlock mode=type.mode left=left right=right output="cmp" nullCompare=false/>
        out.value = cmp == 1 ? 1 : 0;
        <#else>
        out.value = left.value > right.value ? 1 : 0;
        </#if>

        }
    }
  }
  
  @FunctionTemplate(names = {"greater_than_or_equal_to", ">="}, scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class GreaterThanE${left}${right} implements DrillSimpleFunc {

      @Param ${left}Holder left;
      @Param ${right}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        sout: {
        <#if left?starts_with("Nullable")>
        if(left.isSet ==0){
          out.value = 0;
          break sout;
        }
        </#if>
        <#if right?starts_with("Nullable")>
        if(right.isSet ==0){
          out.value = 0;
          break sout;
        }
        </#if>
        
        <#if type.mode == "var" >            
        int cmp;
        <@compareBlock mode=type.mode left=left right=right output="cmp" nullCompare=false/>
        out.value = cmp > -1 ? 1 : 0;
        <#else>
        out.value = left.value >= right.value ? 1 : 0;
        </#if>

        }
      }
  }
  
  @FunctionTemplate(names = {"equal","==","="}, scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Equals${left}${right} implements DrillSimpleFunc {

      @Param ${left}Holder left;
      @Param ${right}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        sout: {
          <#if left?starts_with("Nullable")>
          if(left.isSet ==0){
            out.value = 0;
            break sout;
          }
          </#if>
          <#if right?starts_with("Nullable")>
          if(right.isSet ==0){
            out.value = 0;
            break sout;
          }
          </#if>
        
          <#if type.mode == "var" >
outside: 
        {          
          if (left.end - left.start == right.end - right.start) {
            int n = left.end - left.start;
            int l = left.start;
            int r = right.start;
            while (n-- !=0) {
              byte leftByte = left.buffer.getByte(l++);
              byte rightByte = right.buffer.getByte(r++);
              if (leftByte != rightByte) {
                out.value = 0;
                break outside;
              }
            }
            out.value = 1;
          } else {
            out.value = 0;
          }
        } 
          <#else>
          out.value = left.value == right.value ? 1 : 0;
          </#if>

        }
      }
  }
  
  @FunctionTemplate(names = {"not_equal","<>","!="}, scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class NotEquals${left}${right} implements DrillSimpleFunc {

      @Param ${left}Holder left;
      @Param ${right}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        sout: {
        <#if left?starts_with("Nullable")>
        if(left.isSet ==0){
          out.value = 0;
          break sout;
        }
        </#if>
        <#if right?starts_with("Nullable")>
        if(right.isSet ==0){
          out.value = 0;
          break sout;
        }
        </#if>
        
        <#if type.mode == "var" >            
        int cmp;
        <@compareBlock mode=type.mode left=left right=right output="cmp" nullCompare=false/>
        out.value = cmp == 0 ? 0 : 1;
        <#else>
        out.value = left.value != right.value ? 1 : 0;
        </#if>
        
        }
        
      }
  }
}
</#list>
</#list>
</#list>

