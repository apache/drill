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
package org.apache.drill.exec.store;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

/**
 *  The functions are similar to those created through FreeMarker template for fixed types. There is not much benefit to
 *  using code generation for generating the functions for variable length types, so simply doing them by hand.
 */
public class NewValueFunction {

  @FunctionTemplate(name = "newPartitionValue",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = NullHandling.INTERNAL)
  public static class NewValueVarChar implements DrillSimpleFunc {

    @Param VarCharHolder in;
    @Workspace VarCharHolder previous;
    @Workspace Boolean initialized;
    @Output BitHolder out;
    @Inject DrillBuf buf;

    public void setup() {
      initialized = false;
      previous.buffer = buf;
      previous.start = 0;
    }

    public void eval() {
      int length = in.end - in.start;

      if (initialized) {
        if (org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.compare(previous.buffer, 0, previous.end, in.buffer, in.start, in.end) == 0) {
          out.value = 0;
        } else {
          previous.buffer = buf.reallocIfNeeded(length);
          previous.buffer.setBytes(0, in.buffer, in.start, in.end - in.start);
          previous.end = in.end - in.start;
          out.value = 1;
        }
      } else {
        previous.buffer = buf.reallocIfNeeded(length);
        previous.buffer.setBytes(0, in.buffer, in.start, in.end - in.start);
        previous.end = in.end - in.start;
        out.value = 1;
        initialized = true;
      }
    }
  }

  @FunctionTemplate(name = "newPartitionValue",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = NullHandling.INTERNAL)
  public static class NewValueVarCharNullable implements DrillSimpleFunc {

    @Param NullableVarCharHolder in;
    @Workspace NullableVarCharHolder previous;
    @Workspace Boolean initialized;
    @Output BitHolder out;
    @Inject DrillBuf buf;

    public void setup() {
      initialized = false;
      previous.buffer = buf;
      previous.start = 0;
    }

    public void eval() {
      int length = in.isSet == 0 ? 0 : in.end - in.start;

      if (initialized) {
        if (previous.isSet == 0 && in.isSet == 0 ||
            (org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.compare(
                previous.buffer, 0, previous.end, in.buffer, in.start, in.end) == 0)) {
          out.value = 0;
        } else {
          if (in.isSet == 1) {
            previous.buffer = buf.reallocIfNeeded(length);
            previous.buffer.setBytes(0, in.buffer, in.start, in.end - in.start);
            previous.end = in.end - in.start;
          }
          previous.isSet = in.isSet;
          out.value = 1;
        }
      } else {
        previous.buffer = buf.reallocIfNeeded(length);
        previous.buffer.setBytes(0, in.buffer, in.start, in.end - in.start);
        previous.end = in.end - in.start;
        previous.isSet = 1;
        out.value = 1;
        initialized = true;
      }
    }
  }

  @FunctionTemplate(name = "newPartitionValue",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = NullHandling.INTERNAL)
  public static class NewValueVarBinary implements DrillSimpleFunc {

    @Param VarBinaryHolder in;
    @Workspace VarBinaryHolder previous;
    @Workspace Boolean initialized;
    @Output BitHolder out;
    @Inject DrillBuf buf;

    public void setup() {
      initialized = false;
      previous.buffer = buf;
      previous.start = 0;
    }

    public void eval() {
      int length = in.end - in.start;

      if (initialized) {
        if (org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.compare(previous.buffer, 0, previous.end, in.buffer, in.start, in.end) == 0) {
          out.value = 0;
        } else {
          previous.buffer = buf.reallocIfNeeded(length);
          previous.buffer.setBytes(0, in.buffer, in.start, in.end - in.start);
          previous.end = in.end - in.start;
          out.value = 1;
        }
      } else {
        previous.buffer = buf.reallocIfNeeded(length);
        previous.buffer.setBytes(0, in.buffer, in.start, in.end - in.start);
        previous.end = in.end - in.start;
        out.value = 1;
        initialized = true;
      }
    }
  }

  @FunctionTemplate(name = "newPartitionValue",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = NullHandling.INTERNAL)
  public static class NewValueVarBinaryNullable implements DrillSimpleFunc {

    @Param NullableVarBinaryHolder in;
    @Workspace NullableVarBinaryHolder previous;
    @Workspace Boolean initialized;
    @Output BitHolder out;
    @Inject DrillBuf buf;

    public void setup() {
      initialized = false;
      previous.buffer = buf;
      previous.start = 0;
    }

    public void eval() {
      int length = in.isSet == 0 ? 0 : in.end - in.start;

      if (initialized) {
        if (previous.isSet == 0 && in.isSet == 0 ||
            (org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.compare(
                previous.buffer, 0, previous.end, in.buffer, in.start, in.end) == 0)) {
          out.value = 0;
        } else {
          if (in.isSet == 1) {
            previous.buffer = buf.reallocIfNeeded(length);
            previous.buffer.setBytes(0, in.buffer, in.start, in.end - in.start);
            previous.end = in.end - in.start;
          }
          previous.isSet = in.isSet;
          out.value = 1;
        }
      } else {
        previous.buffer = buf.reallocIfNeeded(length);
        previous.buffer.setBytes(0, in.buffer, in.start, in.end - in.start);
        previous.end = in.end - in.start;
        previous.isSet = 1;
        out.value = 1;
        initialized = true;
      }
    }
  }
}
