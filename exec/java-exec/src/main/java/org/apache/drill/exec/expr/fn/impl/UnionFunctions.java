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
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableTinyIntHolder;
import org.apache.drill.exec.expr.holders.NullableUInt1Holder;
import org.apache.drill.exec.expr.holders.UnionHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.impl.UnionReader;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import javax.inject.Inject;

/**
 * The class contains additional functions for union types in addition to those in GUnionFunctions
 */
public class UnionFunctions {

  @FunctionTemplate(names = {"typeOf"},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = NullHandling.INTERNAL)
  public static class GetType implements DrillSimpleFunc {

    @Param
    FieldReader input;
    @Output
    VarCharHolder out;
    @Inject
    DrillBuf buf;

    public void setup() {}

    public void eval() {

      byte[] type;
      if (input.isSet()) {
         type = input.getType().getMinorType().name().getBytes();
      } else {
        type = org.apache.drill.common.types.TypeProtos.MinorType.NULL.name().getBytes();
      }
      buf = buf.reallocIfNeeded(type.length);
      buf.setBytes(0, type);
      out.buffer = buf;
      out.start = 0;
      out.end = type.length;
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

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "ASSERT_LIST", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL)
  public static class CastUnionList implements DrillSimpleFunc {

    @Param UnionHolder in;
    @Output UnionHolder out;

    public void setup() {}

    public void eval() {
      if (in.isSet == 1) {
        if (in.reader.getType().getMinorType() != org.apache.drill.common.types.TypeProtos.MinorType.LIST) {
          throw new UnsupportedOperationException("The input is not a LIST type");
        }
        out.reader = in.reader;
      } else {
        out.isSet = 0;
      }
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "IS_LIST", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL)
  public static class UnionIsList implements DrillSimpleFunc {

    @Param UnionHolder in;
    @Output BitHolder out;

    public void setup() {}

    public void eval() {
      if (in.isSet == 1) {
        out.value = in.getType().getMinorType() == org.apache.drill.common.types.TypeProtos.MinorType.LIST ? 1 : 0;
      } else {
        out.value = 0;
      }
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "ASSERT_MAP", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL)
  public static class CastUnionMap implements DrillSimpleFunc {

    @Param UnionHolder in;
    @Output UnionHolder out;

    public void setup() {}

    public void eval() {
      if (in.isSet == 1) {
        if (in.reader.getType().getMinorType() != org.apache.drill.common.types.TypeProtos.MinorType.MAP) {
          throw new UnsupportedOperationException("The input is not a MAP type");
        }
        out.reader = in.reader;
      } else {
        out.isSet = 0;
      }
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "IS_MAP", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL)
  public static class UnionIsMap implements DrillSimpleFunc {

    @Param UnionHolder in;
    @Output BitHolder out;

    public void setup() {}

    public void eval() {
      if (in.isSet == 1) {
        out.value = in.getType().getMinorType() == org.apache.drill.common.types.TypeProtos.MinorType.MAP ? 1 : 0;
      } else {
        out.value = 0;
      }
    }
  }

  @FunctionTemplate(names = {"isnotnull", "is not null"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class IsNotNull implements DrillSimpleFunc {

    @Param UnionHolder input;
    @Output BitHolder out;

    public void setup() { }

    public void eval() {
      out.value = input.isSet == 1 ? 1 : 0;
    }
  }

  @FunctionTemplate(names = {"isnull", "is null"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class IsNull implements DrillSimpleFunc {

    @Param UnionHolder input;
    @Output BitHolder out;

    public void setup() { }

    public void eval() {
      out.value = input.isSet == 1 ? 0 : 1;
    }
  }

}
