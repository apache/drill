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

<#list date.dateTypes as type>

<#if type.name == "Date" || type.name == "TimeStamp" || type.name == "Time" || type.name == "TimeStampTZ" || type.name == "IntervalYear">       <#-- type.name is Date, TimeStamp, Time, TimeStampTZ or IntervalYear -->

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/GCompare${type.name}Functions.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

import javax.xml.ws.Holder;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

@SuppressWarnings("unused")
public class GCompare${type.name}Functions {

  @FunctionTemplate(name = "compare_to", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Compare${type.name} implements DrillSimpleFunc {

      @Param ${type.name}Holder left;
      @Param ${type.name}Holder right;
      @Output IntHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {

          out.value = (left.value > right.value) ? 1 : ((left.value < right.value) ? -1 : 0);
      }
  }

  @FunctionTemplate(name = "less_than", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class LessThan${type.name} implements DrillSimpleFunc {

      @Param ${type.name}Holder left;
      @Param ${type.name}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        out.value = left.value < right.value ? 1 : 0;
      }
  }

  @FunctionTemplate(name = "less_than_or_equal_to", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class LessThanE${type.name} implements DrillSimpleFunc {

      @Param ${type.name}Holder left;
      @Param ${type.name}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        out.value = left.value <= right.value ? 1 : 0;
    }
  }

  @FunctionTemplate(name = "greater_than", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class GreaterThan${type.name} implements DrillSimpleFunc {

      @Param ${type.name}Holder left;
      @Param ${type.name}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        out.value = left.value > right.value ? 1 : 0;
    }
  }

  @FunctionTemplate(name = "greater_than_or_equal_to", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class GreaterThanE${type.name} implements DrillSimpleFunc {

      @Param ${type.name}Holder left;
      @Param ${type.name}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        out.value = left.value >= right.value ? 1 : 0;
      }
  }

  @FunctionTemplate(name = "equal", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Equals${type.name} implements DrillSimpleFunc {

      @Param ${type.name}Holder left;
      @Param ${type.name}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        out.value = left.value == right.value ? 1 : 0;
      }
  }

  @FunctionTemplate(name = "not_equal", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class NotEquals${type.name} implements DrillSimpleFunc {

      @Param ${type.name}Holder left;
      @Param ${type.name}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        out.value = left.value != right.value ? 1 : 0;
      }
  }

}

<#elseif type.name == "Interval" || type.name == "IntervalDay">

<#macro intervalCompareBlock left right leftMonths leftDays leftMillis rightMonths rightDays rightMillis output>
outside: {

        org.joda.time.MutableDateTime leftDate  = new org.joda.time.MutableDateTime(1970, 1, 1, 0, 0, 0, 0, org.joda.time.DateTimeZone.UTC);
        org.joda.time.MutableDateTime rightDate = new org.joda.time.MutableDateTime(1970, 1, 1, 0, 0, 0, 0, org.joda.time.DateTimeZone.UTC);

        // Left and right date have the same starting point (epoch), add the interval period and compare the two
        leftDate.addMonths(${leftMonths});
        leftDate.addDays(${leftDays});
        leftDate.add(${leftMillis});

        rightDate.addMonths(${rightMonths});
        rightDate.addDays(${rightDays});
        rightDate.add(${rightMillis});

        long leftMS  = leftDate.getMillis();
        long rightMS = rightDate.getMillis();

        ${output} = ((leftMS < rightMS) ? -1 : ((leftMS > rightMS) ? 1 : 0));

    }
</#macro>

<#macro intervalConvertBlock left right leftMonths leftDays leftMillis rightMonths rightDays rightMillis>
        org.joda.time.MutableDateTime leftDate  = new org.joda.time.MutableDateTime(1970, 1, 1, 0, 0, 0, 0, org.joda.time.DateTimeZone.UTC);
        org.joda.time.MutableDateTime rightDate = new org.joda.time.MutableDateTime(1970, 1, 1, 0, 0, 0, 0, org.joda.time.DateTimeZone.UTC);

        // Left and right date have the same starting point (epoch), add the interval period and compare the two
        leftDate.addMonths(${leftMonths});
        leftDate.addDays(${leftDays});
        leftDate.add(${leftMillis});

        rightDate.addMonths(${rightMonths});
        rightDate.addDays(${rightDays});
        rightDate.add(${rightMillis});

        long leftMS  = leftDate.getMillis();
        long rightMS = rightDate.getMillis();
</#macro>

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/GCompare${type.name}Functions.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

@SuppressWarnings("unused")
public class GCompare${type.name}Functions {

  @FunctionTemplate(name = "compare_to", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class GCCompare${type.name} implements DrillSimpleFunc {

      @Param ${type.name}Holder left;
      @Param ${type.name}Holder right;
      @Output IntHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {

          <#if type.name == "Interval">
          <@intervalCompareBlock left="left" right="right" leftMonths="left.months" leftDays="left.days" leftMillis="left.milliSeconds" rightMonths="right.months" rightDays="right.days" rightMillis="right.milliSeconds" output="out.value"/>
          <#else>
          <@intervalCompareBlock left="left" right="right" leftMonths="0" leftDays="left.days" leftMillis="left.milliSeconds" rightMonths="0" rightDays="right.days" rightMillis="right.milliSeconds" output="out.value"/>
          </#if>

      }
  }

  @FunctionTemplate(name = "less_than", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class LessThan${type.name} implements DrillSimpleFunc {

      @Param ${type.name}Holder left;
      @Param ${type.name}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {

          <#if type.name == "Interval">
          <@intervalConvertBlock left="left" right="right" leftMonths="left.months" leftDays="left.days" leftMillis="left.milliSeconds" rightMonths="right.months" rightDays="right.days" rightMillis="right.milliSeconds"/>
          <#else>
          <@intervalConvertBlock left="left" right="right" leftMonths="0" leftDays="left.days" leftMillis="left.milliSeconds" rightMonths="0" rightDays="right.days" rightMillis="right.milliSeconds"/>
          </#if>

          out.value = leftMS < rightMS ? 1 : 0;
      }
  }

  @FunctionTemplate(name = "less_than_or_equal_to", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class LessThanE${type.name} implements DrillSimpleFunc {

      @Param ${type.name}Holder left;
      @Param ${type.name}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {

          <#if type.name == "Interval">
          <@intervalConvertBlock left="left" right="right" leftMonths="left.months" leftDays="left.days" leftMillis="left.milliSeconds" rightMonths="right.months" rightDays="right.days" rightMillis="right.milliSeconds"/>
          <#else>
          <@intervalConvertBlock left="left" right="right" leftMonths="0" leftDays="left.days" leftMillis="left.milliSeconds" rightMonths="0" rightDays="right.days" rightMillis="right.milliSeconds"/>
          </#if>

          out.value = leftMS <= rightMS ? 1 : 0;
    }
  }

  @FunctionTemplate(name = "greater_than", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class GreaterThan${type.name} implements DrillSimpleFunc {

      @Param ${type.name}Holder left;
      @Param ${type.name}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {

          <#if type.name == "Interval">
          <@intervalConvertBlock left="left" right="right" leftMonths="left.months" leftDays="left.days" leftMillis="left.milliSeconds" rightMonths="right.months" rightDays="right.days" rightMillis="right.milliSeconds"/>
          <#else>
          <@intervalConvertBlock left="left" right="right" leftMonths="0" leftDays="left.days" leftMillis="left.milliSeconds" rightMonths="0" rightDays="right.days" rightMillis="right.milliSeconds"/>
          </#if>

          out.value = leftMS > rightMS ? 1 : 0;
    }
  }

  @FunctionTemplate(name = "greater_than_or_equal_to", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class GreaterThanE${type.name} implements DrillSimpleFunc {

      @Param ${type.name}Holder left;
      @Param ${type.name}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {

          <#if type.name == "Interval">
          <@intervalConvertBlock left="left" right="right" leftMonths="left.months" leftDays="left.days" leftMillis="left.milliSeconds" rightMonths="right.months" rightDays="right.days" rightMillis="right.milliSeconds"/>
          <#else>
          <@intervalConvertBlock left="left" right="right" leftMonths="0" leftDays="left.days" leftMillis="left.milliSeconds" rightMonths="0" rightDays="right.days" rightMillis="right.milliSeconds"/>
          </#if>

          out.value = leftMS >= rightMS ? 1 : 0;
      }
  }

  @FunctionTemplate(name = "equal", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Equals${type.name} implements DrillSimpleFunc {

      @Param ${type.name}Holder left;
      @Param ${type.name}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
          <#if type.name == "Interval">
          <@intervalConvertBlock left="left" right="right" leftMonths="left.months" leftDays="left.days" leftMillis="left.milliSeconds" rightMonths="right.months" rightDays="right.days" rightMillis="right.milliSeconds"/>
          <#else>
          <@intervalConvertBlock left="left" right="right" leftMonths="0" leftDays="left.days" leftMillis="left.milliSeconds" rightMonths="0" rightDays="right.days" rightMillis="right.milliSeconds"/>
          </#if>

          out.value = leftMS == rightMS ? 1 : 0;
      }
  }

  @FunctionTemplate(name = "not_equal", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class NotEquals${type.name} implements DrillSimpleFunc {

      @Param ${type.name}Holder left;
      @Param ${type.name}Holder right;
      @Output BitHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
          <#if type.name == "Interval">
          <@intervalConvertBlock left="left" right="right" leftMonths="left.months" leftDays="left.days" leftMillis="left.milliSeconds" rightMonths="right.months" rightDays="right.days" rightMillis="right.milliSeconds"/>
          <#else>
          <@intervalConvertBlock left="left" right="right" leftMonths="0" leftDays="left.days" leftMillis="left.milliSeconds" rightMonths="0" rightDays="right.days" rightMillis="right.milliSeconds"/>
          </#if>

          out.value = leftMS != rightMS ? 1 : 0;
      }
  }
}
</#if>
</#list>