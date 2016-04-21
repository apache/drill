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
package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.RepeatedBigIntHolder;
import org.apache.arrow.vector.holders.RepeatedBitHolder;
import org.apache.arrow.vector.holders.RepeatedFloat4Holder;
import org.apache.arrow.vector.holders.RepeatedFloat8Holder;
import org.apache.arrow.vector.holders.RepeatedIntHolder;
import org.apache.arrow.vector.holders.RepeatedTinyIntHolder;
import org.apache.arrow.vector.holders.RepeatedVarCharHolder;
import org.apache.arrow.vector.holders.TinyIntHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

public class SimpleRepeatedFunctions {

  private SimpleRepeatedFunctions() {
  }

  // TODO - replace with a freemarker template and fill out the rest of the types
  // focused on getting functions defined for JSON types as this is the primary format
  // users are extracting repeated data out of currently

  @FunctionTemplate(name = "repeated_count", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class RepeatedLengthBigInt implements DrillSimpleFunc {

    @Param
    RepeatedBigIntHolder input;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = input.end - input.start;
    }
  }
  @FunctionTemplate(name = "repeated_count", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class RepeatedLengthTinyInt implements DrillSimpleFunc {

    @Param
    RepeatedTinyIntHolder input;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = input.end - input.start;
    }
  }

  /*
  // TODO - need to confirm that these work   SMP: They do not
  @FunctionTemplate(name = "repeated_count", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class RepeatedLengthMap implements DrillSimpleFunc {

    @Param
    RepeatedMapHolder input;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = input.end - input.start;
    }
  }

  // TODO - need to confirm that these work   SMP: They do not
  @FunctionTemplate(name = "repeated_count", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class RepeatedLengthList implements DrillSimpleFunc {

    @Param
    RepeatedListHolder input;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = input.end - input.start;
    }
  }
  */

  @FunctionTemplate(name = "repeated_count", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class RepeatedLengthBit implements DrillSimpleFunc {

    @Param
    RepeatedBitHolder input;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = input.end - input.start;
    }
  }

  @FunctionTemplate(name = "repeated_count", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class RepeatedLengthFloat4 implements DrillSimpleFunc {

    @Param
    RepeatedFloat4Holder input;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = input.end - input.start;
    }
  }

  @FunctionTemplate(name = "repeated_count", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class RepeatedLengthFloat8 implements DrillSimpleFunc {

    @Param
    RepeatedFloat8Holder input;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = input.end - input.start;
    }
  }

  @FunctionTemplate(name = "repeated_count", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class RepeatedLengthInt implements DrillSimpleFunc {

    @Param RepeatedIntHolder input;
    @Output IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = input.end - input.start;
    }
  }

  @FunctionTemplate(name = "repeated_count", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class RepeatedLengthVarChar implements DrillSimpleFunc {

    @Param RepeatedVarCharHolder input;
    @Output IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = input.end - input.start;
    }
  }

  @FunctionTemplate(name = "repeated_contains", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ContainsBigInt implements DrillSimpleFunc {

    @Param RepeatedBigIntHolder listToSearch;
    @Param BigIntHolder targetValue;
    @Output BitHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = 0;
      for (int i = listToSearch.start; i < listToSearch.end; i++) {
        if (listToSearch.vector.getAccessor().get(i) == targetValue.value) {
          out.value = 1;
          break;
        }
      }
    }

  }

  @FunctionTemplate(name = "repeated_contains", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ContainsInt implements DrillSimpleFunc {

    @Param RepeatedIntHolder listToSearch;
    @Param IntHolder targetValue;
    @Output BitHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = 0;
      for (int i = listToSearch.start; i < listToSearch.end; i++) {
        if (listToSearch.vector.getAccessor().get(i) == targetValue.value) {
          out.value = 1;
          break;
        }
      }
    }

  }

  @FunctionTemplate(name = "repeated_contains", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ContainsTinyInt implements DrillSimpleFunc {

    @Param RepeatedTinyIntHolder listToSearch;
    @Param TinyIntHolder targetValue;
    @Output BitHolder out;

    public void setup() {
    }

    public void eval() {
      for (int i = listToSearch.start; i < listToSearch.end; i++) {
        if (listToSearch.vector.getAccessor().get(i) == targetValue.value) {
          out.value = 1;
          break;
        }
      }
    }

  }

  @FunctionTemplate(name = "repeated_contains", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ContainsBit implements DrillSimpleFunc {

    @Param RepeatedBitHolder listToSearch;
    @Param BitHolder targetValue;
    @Output BitHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = 0;
      for (int i = listToSearch.start; i < listToSearch.end; i++) {
        if (listToSearch.vector.getAccessor().get(i) == targetValue.value) {
          out.value = 1;
          break;
        }
      }
    }

  }

  @FunctionTemplate(name = "repeated_contains", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ContainsFloat4 implements DrillSimpleFunc {

    @Param RepeatedFloat4Holder listToSearch;
    @Param Float4Holder targetValue;
    @Output BitHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = 0;
      for (int i = listToSearch.start; i < listToSearch.end; i++) {
        if (listToSearch.vector.getAccessor().get(i) == targetValue.value) {
          out.value = 1;
          break;
        }
      }
    }

  }

  @FunctionTemplate(name = "repeated_contains", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ContainsFloat8 implements DrillSimpleFunc {

    @Param RepeatedFloat8Holder listToSearch;
    @Param Float8Holder targetValue;
    @Output BitHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = 0;
      for (int i = listToSearch.start; i < listToSearch.end; i++) {
        if (listToSearch.vector.getAccessor().get(i) == targetValue.value) {
          out.value = 1;
          break;
        }
      }
    }

  }

  @FunctionTemplate(name = "repeated_contains", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ContainsVarChar implements DrillSimpleFunc {

    @Param RepeatedVarCharHolder listToSearch;
    @Param VarCharHolder targetValue;
    @Workspace VarCharHolder currVal;
    @Workspace java.util.regex.Matcher matcher;

    @Output BitHolder out;

    public void setup() {
      currVal = new VarCharHolder();
      matcher = java.util.regex.Pattern.compile(
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(targetValue.start,  targetValue.end,  targetValue.buffer)).matcher("");
    }

    public void eval() {
      for (int i = listToSearch.start; i < listToSearch.end; i++) {
        out.value = 0;
        listToSearch.vector.getAccessor().get(i, currVal);
        String in = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(currVal.start, currVal.end, currVal.buffer);
        matcher.reset(in);
          if(matcher.find()) {
             out.value = 1;
             break;
          }
       }
    }
  }
}