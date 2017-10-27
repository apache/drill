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
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal18Holder;
import org.apache.drill.exec.expr.holders.NullableDecimal28SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal38SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal9Holder;
import org.apache.drill.exec.expr.holders.NullableFloat4Holder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;

/**
 * hash32 function definitions for numeric data types. These functions cast the input numeric value to a
 * double before doing the hashing. See comments in {@link Hash64AsDouble} for the reason for doing this.
 */
public class Hash32AsDouble {
  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)

  public static class NullableFloatHash implements DrillSimpleFunc {

    @Param
    NullableFloat4Holder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(in.value, 0);
      }
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class FloatHash implements DrillSimpleFunc {

    @Param
    Float4Holder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(in.value, 0);
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDoubleHash implements DrillSimpleFunc {

    @Param
    NullableFloat8Holder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(in.value, 0);
      }
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class DoubleHash implements DrillSimpleFunc {

    @Param
    Float8Holder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(in.value, 0);
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableBigIntHash implements DrillSimpleFunc {

    @Param
    NullableBigIntHolder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32((long) in.value, 0);
      }
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableIntHash implements DrillSimpleFunc {
    @Param
    NullableIntHolder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(in.value, 0);
      }
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class BigIntHash implements DrillSimpleFunc {

    @Param
    BigIntHolder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32((long)in.value, 0);
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class IntHash implements DrillSimpleFunc {
    @Param
    IntHolder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(in.value, 0);
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class Decimal9Hash implements DrillSimpleFunc {
    @Param
    Decimal9Holder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      java.math.BigDecimal input = new java.math.BigDecimal(java.math.BigInteger.valueOf(in.value), in.scale);
      out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(input.doubleValue(), 0);
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDecimal9Hash implements DrillSimpleFunc {
    @Param
    NullableDecimal9Holder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        java.math.BigDecimal input = new java.math.BigDecimal(java.math.BigInteger.valueOf(in.value), in.scale);
        out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(input.doubleValue(), 0);
      }
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class Decimal18Hash implements DrillSimpleFunc {
    @Param
    Decimal18Holder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      java.math.BigDecimal input = new java.math.BigDecimal(java.math.BigInteger.valueOf(in.value), in.scale);
      out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(input.doubleValue(), 0);
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDecimal18Hash implements DrillSimpleFunc {
    @Param
    NullableDecimal18Holder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        java.math.BigDecimal input = new java.math.BigDecimal(java.math.BigInteger.valueOf(in.value), in.scale);
        out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(input.doubleValue(), 0);
      }
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class Decimal28Hash implements DrillSimpleFunc {
    @Param
    Decimal28SparseHolder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      java.math.BigDecimal input = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(in.buffer,
          in.start, in.nDecimalDigits, in.scale);
      out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(input.doubleValue(), 0);
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDecimal28Hash implements DrillSimpleFunc {
    @Param
    NullableDecimal28SparseHolder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        java.math.BigDecimal input = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(in.buffer,
            in.start, in.nDecimalDigits, in.scale);
        out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(input.doubleValue(), 0);
      }
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class Decimal38Hash implements DrillSimpleFunc {
    @Param
    Decimal38SparseHolder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      java.math.BigDecimal input = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(in.buffer,
          in.start, in.nDecimalDigits, in.scale);
      out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(input.doubleValue(), 0);
    }
  }

  @FunctionTemplate(name = "hash32AsDouble", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class NullableDecimal38Hash implements DrillSimpleFunc {
    @Param
    NullableDecimal38SparseHolder in;
    @Output
    IntHolder out;

    public void setup() {
    }

    public void eval() {
      if (in.isSet == 0) {
        out.value = 0;
      } else {
        java.math.BigDecimal input = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(in.buffer,
            in.start, in.nDecimalDigits, in.scale);
        out.value = org.apache.drill.exec.expr.fn.impl.HashHelper.hash32(input.doubleValue(), 0);
      }
    }
  }
}
