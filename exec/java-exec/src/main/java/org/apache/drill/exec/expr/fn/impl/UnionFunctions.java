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

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.UnionHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.impl.UnionReader;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import javax.inject.Inject;

public class UnionFunctions {

  @FunctionTemplate(names = {"typeString"},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = NullHandling.NULL_IF_NULL)
  public static class FromType implements DrillSimpleFunc {

    @Param
    IntHolder in;
    @Output
    VarCharHolder out;
    @Inject
    DrillBuf buffer;

    public void setup() {}

    public void eval() {

      VarCharHolder h = org.apache.drill.exec.vector.ValueHolderHelper.getVarCharHolder(buffer, org.apache.drill.common.types.MinorType.valueOf(in.value).toString());
      out.buffer = h.buffer;
      out.start = h.start;
      out.end = h.end;
    }
  }

  @FunctionTemplate(names = {"type"},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = NullHandling.NULL_IF_NULL)
  public static class ToType implements DrillSimpleFunc {

    @Param
    VarCharHolder input;
    @Output
    IntHolder out;

    public void setup() {}

    public void eval() {

      out.value = input.getType().getMinorType().getNumber();
      byte[] b = new byte[input.end - input.start];
      input.buffer.getBytes(input.start, b, 0, b.length);
      String type = new String(b);
      out.value = org.apache.drill.common.types.MinorType.valueOf(type.toUpperCase()).getNumber();
    }
  }

  @FunctionTemplate(names = {"typeOf"},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = NullHandling.INTERNAL)
  public static class GetType implements DrillSimpleFunc {

    @Param
    FieldReader input;
    @Output
    IntHolder out;

    public void setup() {}

    public void eval() {

      out.value = input.isSet() ? input.getType().getMinorType().getNumber() : 0;

    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"castUNION", "castToUnion"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastUnionToUnion implements DrillSimpleFunc{

    @Param FieldReader in;
    @Output
    UnionHolder out;

    public void setup() {}

    public void eval() {
      out.reader = in;
      out.isSet = in.isSet() ? 1 : 0;
    }
  }
}
