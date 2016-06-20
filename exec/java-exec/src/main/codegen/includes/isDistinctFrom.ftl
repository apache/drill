<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License. -->

<#macro intervalCompareBlock leftType rightType leftMonths leftDays leftMillis rightMonths rightDays rightMillis output>

        org.joda.time.MutableDateTime leftDate  =
            new org.joda.time.MutableDateTime(1970, 1, 1, 0, 0, 0, 0, org.joda.time.DateTimeZone.UTC);
        org.joda.time.MutableDateTime rightDate =
            new org.joda.time.MutableDateTime(1970, 1, 1, 0, 0, 0, 0, org.joda.time.DateTimeZone.UTC);

        // Left and right date have the same starting point (epoch), add the interval period and compare the two
        leftDate.addMonths(${leftMonths});
        leftDate.addDays(${leftDays});
        leftDate.add(${leftMillis});

        rightDate.addMonths(${rightMonths});
        rightDate.addDays(${rightDays});
        rightDate.add(${rightMillis});

        long leftMS  = leftDate.getMillis();
        long rightMS = rightDate.getMillis();

        ${output} = leftMS < rightMS ? -1 : (leftMS > rightMS ? 1 : 0);

</#macro>

<#-- macro isDistinctFromMacro: generate eval block for handling IS_DISTINCT_FROM or IS_NOT_DISTINCT_FROM functions -->
<#-- Parameters: -->
<#--  - leftType: data type of the left argument -->
<#--  - rightType: data type of the right argument -->
<#--  - mode: type of data (string, primitive or interval etc.) -->
<--   - isDistinctFrom: true for IS_DISTINCT_FROM function, false for IS_NOT_DISTINCT_FROM function -->
<#macro isDistinctFromMacro leftType rightType mode isNotDistinct>
    <#if isNotDistinct>
      <#assign equalResult = 1> <#-- Result when left and right are equal -->
      <#assign notEqualResult = 0> <#-- Result when left and right are not equal -->
    <#else>
      <#assign equalResult = 0> <#-- Result when left and right are equal -->
      <#assign notEqualResult = 1> <#-- Result when left and right are not equal -->
    </#if>
      outside:{
        <#if leftType?starts_with("Nullable")>
          <#if rightType?starts_with("Nullable")>
            <#-- Left and Right are NULLABLE types -->
        if(left.isSet==0){
          if(right.isSet==0){
            out.value=${equalResult};
            break outside;
          }else{
            out.value=${notEqualResult};
            break outside;
          }
        } else {
          if(right.isSet==0){
            out.value=${notEqualResult};
            break outside;
          }
        }
          <#else>
            <#-- Left is NULLABLE type and Right is REQUIRED type -->
        if(left.isSet==0){
          out.value=${notEqualResult};
          break outside;
        }
          </#if>
        <#else>
          <#if rightType?starts_with("Nullable")>
            <#-- Left is REQUIRED type and Right is NULLABLE type -->
        if(right.isSet==0){
          out.value=${notEqualResult};
          break outside;
        }
          <#else>
            <#-- Left and Right are REQUIRED types -->
          </#if>
        </#if>

        <#-- At this point we know that both left and right have a non-null value -->
        <#if mode == "primitive">
        out.value = left.value == right.value ? ${equalResult} : ${notEqualResult};
        <#elseif mode == "varString">
        out.value = org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.compare(
            left.buffer, left.start, left.end, right.buffer, right.start, right.end) == 0 ? ${equalResult} : ${notEqualResult};
        <#elseif mode == "decimal">
        out.value = org.apache.drill.exec.util.DecimalUtility.compareSparseBytes(
            left.buffer, left.start, left.getSign(left.start, left.buffer), left.scale, left.precision, right.buffer,
            right.start, right.getSign(right.start, right.buffer), right.precision, right.scale, left.WIDTH, left.nDecimalDigits, false)
            == 0 ? ${equalResult} : ${notEqualResult};
        <#elseif mode == "intervalNameThis">
        int comparisionResult;
          <@intervalCompareBlock leftType=leftType rightType=rightType
            leftMonths ="left.months"  leftDays ="left.days"  leftMillis ="left.milliseconds"
            rightMonths="right.months" rightDays="right.days" rightMillis="right.milliseconds"
          output="comparisionResult"/>
        out.value = comparisionResult == 0 ? ${equalResult} : ${notEqualResult};
        <#elseif mode == "intervalDay">
        int comparisionResult;
          <@intervalCompareBlock leftType=leftType rightType=rightType
            leftMonths ="0" leftDays ="left.days"  leftMillis ="left.milliseconds"
            rightMonths="0" rightDays="right.days" rightMillis="right.milliseconds"
          output="comparisionResult"/>
        out.value = comparisionResult == 0 ? ${equalResult} : ${notEqualResult};
        <#else>
          ${mode_HAS_BAD_VALUE}
        </#if>
      }
</#macro>
