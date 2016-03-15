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
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.NullableDateHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal18Holder;
import org.apache.drill.exec.expr.holders.NullableDecimal28SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal38SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal9Holder;
import org.apache.drill.exec.expr.holders.NullableFloat4Holder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableTimeHolder;
import org.apache.drill.exec.expr.holders.NullableTimeStampHolder;
import org.apache.drill.exec.expr.holders.NullableVar16CharHolder;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.Var16CharHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

/*
 * Class contains hash32 function definitions for different data types.
 */
public class Hash32FunctionsWithSeed {
  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class NullableFloatHash implements DrillSimpleFunc {

    @Param NullableFloat4Holder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class FloatHash implements DrillSimpleFunc {

    @Param Float4Holder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class NullableDoubleHash implements DrillSimpleFunc {

    @Param NullableFloat8Holder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class DoubleHash implements DrillSimpleFunc {

    @Param Float8Holder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class NullableVarBinaryHash implements DrillSimpleFunc {

    @Param NullableVarBinaryHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.start, in.end, in.buffer, seed.value);
      }
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class NullableVarCharHash implements DrillSimpleFunc {

    @Param NullableVarCharHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.start, in.end, in.buffer, seed.value);
      }
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class NullableVar16CharHash implements DrillSimpleFunc {

    @Param NullableVar16CharHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.start, in.end, in.buffer, seed.value);
      }
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableBigIntHash implements DrillSimpleFunc {

    @Param NullableBigIntHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      }
      else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableIntHash implements DrillSimpleFunc {
    @Param NullableIntHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      }
      else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class VarBinaryHash implements DrillSimpleFunc {

    @Param VarBinaryHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.start, in.end, in.buffer, seed.value);
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class VarCharHash implements DrillSimpleFunc {

    @Param VarCharHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.start, in.end, in.buffer, seed.value);
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class Var16CharHash implements DrillSimpleFunc {

    @Param Var16CharHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.start, in.end, in.buffer, seed.value);
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class BigIntHash implements DrillSimpleFunc {

    @Param BigIntHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class IntHash implements DrillSimpleFunc {
    @Param IntHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      // TODO: implement hash function for other types
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
    }
  }
  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class DateHash implements DrillSimpleFunc {
    @Param  DateHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDateHash implements DrillSimpleFunc {
    @Param  NullableDateHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class TimeStampHash implements DrillSimpleFunc {
    @Param  TimeStampHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableTimeStampHash implements DrillSimpleFunc {
    @Param  NullableTimeStampHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class TimeHash implements DrillSimpleFunc {
    @Param  TimeHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableTimeHash implements DrillSimpleFunc {
    @Param  NullableTimeHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class Decimal9Hash implements DrillSimpleFunc {
    @Param  Decimal9Holder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDecimal9Hash implements DrillSimpleFunc {
    @Param  NullableDecimal9Holder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class Decimal18Hash implements DrillSimpleFunc {
    @Param  Decimal18Holder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDecimal18Hash implements DrillSimpleFunc {
    @Param  NullableDecimal18Holder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class Decimal28Hash implements DrillSimpleFunc {
    @Param  Decimal28SparseHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.start, in.start + Decimal28SparseHolder.WIDTH, in.buffer, seed.value);
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDecimal28Hash implements DrillSimpleFunc {
    @Param  NullableDecimal28SparseHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.start, in.start + NullableDecimal28SparseHolder.WIDTH, in.buffer, seed.value);
      }
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class Decimal38Hash implements DrillSimpleFunc {
    @Param  Decimal38SparseHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.start, in.start + Decimal38SparseHolder.WIDTH, in.buffer, seed.value);
    }
  }

  @FunctionTemplate(name = "hash32", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDecimal38Hash implements DrillSimpleFunc {
    @Param  NullableDecimal38SparseHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.start, in.start + NullableDecimal38SparseHolder.WIDTH, in.buffer, seed.value);
      }
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class NullableBitHash implements DrillSimpleFunc {

    @Param NullableBitHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = seed.value;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
      }
    }
  }

  @FunctionTemplate(names = {"hash32", "hash32AsDouble"}, scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL )
  public static class BitHash implements DrillSimpleFunc {

    @Param BitHolder in;
    @Param IntHolder seed;
    @Output IntHolder out;


    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.XXHash.hash32(in.value, seed.value);
    }
  }}
