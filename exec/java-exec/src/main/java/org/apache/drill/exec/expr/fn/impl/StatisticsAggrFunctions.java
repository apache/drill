/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

/*
 * This class is automatically generated from AggrTypeFunctions2.tdd using FreeMarker.
 */

package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal28DenseHolder;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38DenseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.IntervalDayHolder;
import org.apache.drill.exec.expr.holders.IntervalHolder;
import org.apache.drill.exec.expr.holders.IntervalYearHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.NullableDateHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal18Holder;
import org.apache.drill.exec.expr.holders.NullableDecimal28DenseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal28SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal38DenseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal38SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal9Holder;
import org.apache.drill.exec.expr.holders.NullableFloat4Holder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableIntervalDayHolder;
import org.apache.drill.exec.expr.holders.NullableIntervalHolder;
import org.apache.drill.exec.expr.holders.NullableIntervalYearHolder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableTimeHolder;
import org.apache.drill.exec.expr.holders.NullableTimeStampHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.NullableVar16CharHolder;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.MapHolder;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.expr.holders.Var16CharHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.ops.ContextInformation;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import javax.inject.Inject;

@SuppressWarnings("unused")
public class StatisticsAggrFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatisticsAggrFunctions.class);

  @FunctionTemplate(name = "statcount", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class StatCount implements DrillAggFunc {
    @Param FieldReader in;
    @Workspace BigIntHolder count;
    @Output NullableBigIntHolder out;

    @Override
    public void setup() {
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      count.value++;
    }

    @Override
    public void output() {
      out.isSet = 1;
      out.value = count.value;
    }

    @Override
    public void reset() {
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "nonnullstatcount", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NonNullStatCount implements DrillAggFunc {
    @Param FieldReader in;
    @Workspace BigIntHolder count;
    @Output NullableBigIntHolder out;

    @Override
    public void setup() {
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet()) {
        count.value++;
      }
    }

    @Override
    public void output() {
      out.isSet = 1;
      out.value = count.value;
    }

    @Override
    public void reset() {
      count.value = 0;
    }
  }

  /**
   * The log2m parameter defines the accuracy of the counter.  The larger the
   * log2m the better the accuracy.
   * accuracy = 1.04/sqrt(2^log2m)
   * where
   * log2m - the number of bits to use as the basis for the HLL instance
   */
  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class HllFieldReader implements DrillAggFunc {
    @Param FieldReader in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
            (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        int mode = in.getType().getMode().getNumber();
        int type = in.getType().getMinorType().getNumber();

        switch (mode) {
          case org.apache.drill.common.types.TypeProtos.DataMode.OPTIONAL_VALUE:
            if (!in.isSet()) {
              hll.offer(null);
              break;
            }
            // fall through //
          case org.apache.drill.common.types.TypeProtos.DataMode.REQUIRED_VALUE:
            switch (type) {
              case org.apache.drill.common.types.TypeProtos.MinorType.VARCHAR_VALUE:
                hll.offer(in.readText().toString());
                break;
              case org.apache.drill.common.types.TypeProtos.MinorType.BIGINT_VALUE:
                hll.offer(in.readLong());
                break;
              default:
                work.obj = null;
            }
            break;
          default:
            work.obj = null;
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
            (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class BitNDVFunction implements DrillAggFunc {
    @Param
    BitHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBitNDVFunction implements DrillAggFunc {
    @Param
    NullableBitHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntNDVFunction implements DrillAggFunc {
    @Param
    IntHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntNDVFunction implements DrillAggFunc {
    @Param
    NullableIntHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class BigIntNDVFunction implements DrillAggFunc {
    @Param
    BigIntHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBigIntNDVFunction implements DrillAggFunc {
    @Param
    NullableBigIntHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Float4NDVFunction implements DrillAggFunc {
    @Param
    Float4Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat4NDVFunction implements DrillAggFunc {
    @Param
    NullableFloat4Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Float8NDVFunction implements DrillAggFunc {
    @Param
    Float8Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat8NDVFunction implements DrillAggFunc {
    @Param
    NullableFloat8Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal9NDVFunction implements DrillAggFunc {
    @Param
    Decimal9Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal9NDVFunction implements DrillAggFunc {
    @Param
    NullableDecimal9Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal18NDVFunction implements DrillAggFunc {
    @Param
    Decimal18Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal18NDVFunction implements DrillAggFunc {
    @Param
    NullableDecimal18Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class DateNDVFunction implements DrillAggFunc {
    @Param
    DateHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDateNDVFunction implements DrillAggFunc {
    @Param
    NullableDateHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TimeNDVFunction implements DrillAggFunc {
    @Param
    TimeHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeNDVFunction implements DrillAggFunc {
    @Param
    NullableTimeHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TimeStampNDVFunction implements DrillAggFunc {
    @Param
    TimeStampHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeStampNDVFunction implements DrillAggFunc {
    @Param
    NullableTimeStampHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntervalDayNDVFunction implements DrillAggFunc {
    @Param
    IntervalDayHolder in;
    @Workspace
    ObjectHolder work;
    @Workspace
    ObjectHolder interval;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      interval = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[2];
    }

    @Override
    public void add() {
      if (work.obj != null
              && interval.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        ((int[])interval.obj)[0] = in.days;
        ((int[])interval.obj)[1] = in.milliseconds;
        hll.offer(interval.obj);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[2];
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntervalDayNDVFunction implements DrillAggFunc {
    @Param
    NullableIntervalDayHolder in;
    @Workspace
    ObjectHolder work;
    @Workspace
    ObjectHolder interval;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      interval = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[2];
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          if (interval.obj != null) {
            ((int[]) interval.obj)[0] = in.days;
            ((int[]) interval.obj)[1] = in.milliseconds;
            hll.offer(interval.obj);
          }
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[2];
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntervalNDVFunction implements DrillAggFunc {
    @Param
    IntervalHolder in;
    @Workspace
    ObjectHolder work;
    @Workspace
    ObjectHolder interval;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      interval = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[3];
    }

    @Override
    public void add() {
      if (work.obj != null
              && interval.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        ((int[])interval.obj)[0] = in.days;
        ((int[])interval.obj)[1] = in.months;
        ((int[])interval.obj)[2] = in.milliseconds;
        hll.offer(interval.obj);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[3];
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntervalNDVFunction implements DrillAggFunc {
    @Param
    NullableIntervalHolder in;
    @Workspace
    ObjectHolder work;
    @Workspace
    ObjectHolder interval;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      interval = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[3];
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          if (interval.obj != null) {
            ((int[]) interval.obj)[0] = in.days;
            ((int[]) interval.obj)[1] = in.months;
            ((int[]) interval.obj)[2] = in.milliseconds;
            hll.offer(interval.obj);
          }
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[3];
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntervalYearNDVFunction implements DrillAggFunc {
    @Param
    IntervalYearHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntervalYearNDVFunction implements DrillAggFunc {
    @Param
    NullableIntervalYearHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class VarCharNDVFunction implements DrillAggFunc {
    @Param
    VarCharHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        byte[] buf = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
                in.start, in.end, in.buffer).getBytes();
        hll.offer(buf);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarCharNDVFunction implements DrillAggFunc {
    @Param
    NullableVarCharHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          byte[] buf = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
                  in.start, in.end, in.buffer).getBytes();
          hll.offer(buf);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Var16CharNDVFunction implements DrillAggFunc {
    @Param
    Var16CharHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        byte[] buf = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF16
                (in.start, in.end, in.buffer).getBytes();
        hll.offer(buf);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVar16CharNDVFunction implements DrillAggFunc {
    @Param
    NullableVar16CharHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          byte[] buf = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF16
                  (in.start, in.end, in.buffer).getBytes();
          hll.offer(buf);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class VarBinaryNDVFunction implements DrillAggFunc {
    @Param
    VarBinaryHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        byte[] buf = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8
                (in.start, in.end, in.buffer).getBytes();
        hll.offer(buf);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarBinaryNDVFunction implements DrillAggFunc {
    @Param
    NullableVarBinaryHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          byte[] buf = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8
                  (in.start, in.end, in.buffer).getBytes();
          hll.offer(buf);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll_decode", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class HllDecode implements DrillSimpleFunc {

    @Param
    NullableVarBinaryHolder in;
    @Output
    BigIntHolder out;

    @Override
    public void setup() {
    }

    public void eval() {
      out.value = -1;

      if (in.isSet != 0) {
        byte[] din = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, din);
        try {
          out.value = com.clearspring.analytics.stream.cardinality.HyperLogLog.Builder.build(din).cardinality();
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failure evaluating hll_decode", e);
        }
      }
    }
  }

  @FunctionTemplate(name = "hll_merge", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class HllMerge implements DrillAggFunc {
    @Param NullableVarBinaryHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      /**
       * The log2m parameter defines the accuracy of the counter.  The larger the
       * log2m the better the accuracy.
       * accuracy = 1.04/sqrt(2^log2m)
       * where
       * log2m - the number of bits to use as the basis for the HLL instance
       */
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        int mode = in.getType().getMode().getNumber();
        int type = in.getType().getMinorType().getNumber();

        switch (mode) {
          case org.apache.drill.common.types.TypeProtos.DataMode.OPTIONAL_VALUE:
            if (in.isSet == 0) { // No hll structure to merge
              break;
            }
            // fall through //
          case org.apache.drill.common.types.TypeProtos.DataMode.REQUIRED_VALUE:
            try {
              byte[] buf = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
                      in.start, in.end, in.buffer).getBytes();
              com.clearspring.analytics.stream.cardinality.HyperLogLog other =
                  com.clearspring.analytics.stream.cardinality.HyperLogLog.Builder.build(buf);
              hll.addAll(other);
            } catch (Exception e) {
              throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to merge HyperLogLog output", e);
            }
            break;
          default:
            work.obj = null;
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  /*@FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class BitHLLFunction implements DrillAggFunc {
    @Param
    BitHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBitHLLFunction implements DrillAggFunc {
    @Param
    NullableBitHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntHLLFunction implements DrillAggFunc {
    @Param
    IntHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntHLLFunction implements DrillAggFunc {
    @Param
    NullableIntHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class BigIntHLLFunction implements DrillAggFunc {
    @Param
    BigIntHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBigIntHLLFunction implements DrillAggFunc {
    @Param
    NullableBigIntHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Float4HLLFunction implements DrillAggFunc {
    @Param
    Float4Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat4HLLFunction implements DrillAggFunc {
    @Param
    NullableFloat4Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Float8HLLFunction implements DrillAggFunc {
    @Param
    Float8Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat8HLLFunction implements DrillAggFunc {
    @Param
    NullableFloat8Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal9HLLFunction implements DrillAggFunc {
    @Param
    Decimal9Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal9HLLFunction implements DrillAggFunc {
    @Param
    NullableDecimal9Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal18HLLFunction implements DrillAggFunc {
    @Param
    Decimal18Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal18HLLFunction implements DrillAggFunc {
    @Param
    NullableDecimal18Holder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class DateHLLFunction implements DrillAggFunc {
    @Param
    DateHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDateHLLFunction implements DrillAggFunc {
    @Param
    NullableDateHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TimeHLLFunction implements DrillAggFunc {
    @Param
    TimeHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeHLLFunction implements DrillAggFunc {
    @Param
    NullableTimeHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TimeStampHLLFunction implements DrillAggFunc {
    @Param
    TimeStampHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeStampHLLFunction implements DrillAggFunc {
    @Param
    NullableTimeStampHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntervalDayHLLFunction implements DrillAggFunc {
    @Param
    IntervalDayHolder in;
    @Workspace
    ObjectHolder work;
    @Workspace
    ObjectHolder interval;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      interval = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[2];
    }

    @Override
    public void add() {
      if (work.obj != null
              && interval.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        ((int[])interval.obj)[0] = in.days;
        ((int[])interval.obj)[1] = in.milliseconds;
        hll.offer(interval.obj);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[2];
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntervalDayHLLFunction implements DrillAggFunc {
    @Param
    NullableIntervalDayHolder in;
    @Workspace
    ObjectHolder work;
    @Workspace
    ObjectHolder interval;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      interval = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[2];
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          if (interval.obj != null) {
            ((int[]) interval.obj)[0] = in.days;
            ((int[]) interval.obj)[1] = in.milliseconds;
            hll.offer(interval.obj);
          }
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[2];
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntervalHLLFunction implements DrillAggFunc {
    @Param
    IntervalHolder in;
    @Workspace
    ObjectHolder work;
    @Workspace
    ObjectHolder interval;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      interval = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[3];
    }

    @Override
    public void add() {
      if (work.obj != null
          && interval.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        ((int[])interval.obj)[0] = in.days;
        ((int[])interval.obj)[1] = in.months;
        ((int[])interval.obj)[2] = in.milliseconds;
        hll.offer(interval.obj);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[3];
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntervalHLLFunction implements DrillAggFunc {
    @Param
    NullableIntervalHolder in;
    @Workspace
    ObjectHolder work;
    @Workspace
    ObjectHolder interval;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      interval = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[3];
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          if (interval.obj != null) {
            ((int[]) interval.obj)[0] = in.days;
            ((int[]) interval.obj)[1] = in.months;
            ((int[]) interval.obj)[2] = in.milliseconds;
            hll.offer(interval.obj);
          }
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
      interval.obj = new int[3];
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntervalYearHLLFunction implements DrillAggFunc {
    @Param
    IntervalYearHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        hll.offer(in.value);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntervalYearHLLFunction implements DrillAggFunc {
    @Param
    NullableIntervalYearHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          hll.offer(in.value);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class VarCharHLLFunction implements DrillAggFunc {
    @Param
    VarCharHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        byte[] buf = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
                in.start, in.end, in.buffer).getBytes();
        hll.offer(buf);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarCharHLLFunction implements DrillAggFunc {
    @Param
    NullableVarCharHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          byte[] buf = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
                  in.start, in.end, in.buffer).getBytes();
          hll.offer(buf);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Var16CharHLLFunction implements DrillAggFunc {
    @Param
    Var16CharHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        byte[] buf = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF16
                (in.start, in.end, in.buffer).getBytes();
        hll.offer(buf);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVar16CharHLLFunction implements DrillAggFunc {
    @Param
    NullableVar16CharHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          byte[] buf = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF16
                  (in.start, in.end, in.buffer).getBytes();
          hll.offer(buf);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class VarBinaryHLLFunction implements DrillAggFunc {
    @Param
    VarBinaryHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        byte[] buf = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8
                (in.start, in.end, in.buffer).getBytes();
        hll.offer(buf);
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarBinaryHLLFunction implements DrillAggFunc {
    @Param
    NullableVarBinaryHolder in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableVarBinaryHolder out;
    @Inject ContextInformation contextInfo;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        if (in.isSet == 1) {
          byte[] buf = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8
                  (in.start, in.end, in.buffer).getBytes();
          hll.offer(buf);
        } else {
          hll.offer(null);
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
                (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        try {
          byte[] ba = hll.getBytes();
          out.buffer = buffer.reallocIfNeeded(ba.length);
          out.start = 0;
          out.end = ba.length;
          out.buffer.setBytes(0, ba);
          out.isSet = 1;
        } catch (java.io.IOException e) {
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failed to get HyperLogLog output", e);
        }
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }*/
  /*@FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NdvVarBinary implements DrillAggFunc {
    @Param
    FieldReader in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;
    @Inject ContextInformation contextInfo;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }

    @Override
    public void add() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
            (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
        int mode = in.getType().getMode().getNumber();
        int type = in.getType().getMinorType().getNumber();

        switch (mode) {
          case org.apache.drill.common.types.TypeProtos.DataMode.OPTIONAL_VALUE:
            if (!in.isSet()) {
              hll.offer(null);
              break;
            }
            // fall through //
          case org.apache.drill.common.types.TypeProtos.DataMode.REQUIRED_VALUE:
            switch (type) {
              case org.apache.drill.common.types.TypeProtos.MinorType.BIT_VALUE:
                hll.offer(in.readBoolean());
                break;
              case org.apache.drill.common.types.TypeProtos.MinorType.DECIMAL9_VALUE:
              case org.apache.drill.common.types.TypeProtos.MinorType.DECIMAL18_VALUE:
              case org.apache.drill.common.types.TypeProtos.MinorType.DECIMAL28DENSE_VALUE:
              case org.apache.drill.common.types.TypeProtos.MinorType.DECIMAL28SPARSE_VALUE:
              case org.apache.drill.common.types.TypeProtos.MinorType.DECIMAL38DENSE_VALUE:
              case org.apache.drill.common.types.TypeProtos.MinorType.DECIMAL38SPARSE_VALUE:
                hll.offer(in.readBigDecimal());
                break;
              case org.apache.drill.common.types.TypeProtos.MinorType.FLOAT4_VALUE:
                hll.offer(in.readFloat());
                break;
              case org.apache.drill.common.types.TypeProtos.MinorType.FLOAT8_VALUE:
                hll.offer(in.readDouble());
                break;
              case org.apache.drill.common.types.TypeProtos.MinorType.INT_VALUE:
                hll.offer(in.readInteger());
                break;
              case org.apache.drill.common.types.TypeProtos.MinorType.BIGINT_VALUE:
                hll.offer(in.readLong());
                break;
              case org.apache.drill.common.types.TypeProtos.MinorType.DATE_VALUE:
              case org.apache.drill.common.types.TypeProtos.MinorType.TIME_VALUE:
              case org.apache.drill.common.types.TypeProtos.MinorType.TIMETZ_VALUE:
              case org.apache.drill.common.types.TypeProtos.MinorType.TIMESTAMP_VALUE:
              case org.apache.drill.common.types.TypeProtos.MinorType.TIMESTAMPTZ_VALUE:
                hll.offer(in.readDateTime());
                break;
              case org.apache.drill.common.types.TypeProtos.MinorType.INTERVAL_VALUE:
              case org.apache.drill.common.types.TypeProtos.MinorType.INTERVALDAY_VALUE:
              case org.apache.drill.common.types.TypeProtos.MinorType.INTERVALYEAR_VALUE:
                hll.offer(in.readByteArray());
                break;
              case org.apache.drill.common.types.TypeProtos.MinorType.VARBINARY_VALUE:
                hll.offer(in.readByteArray());
                break;
              case org.apache.drill.common.types.TypeProtos.MinorType.VARCHAR_VALUE:
              case org.apache.drill.common.types.TypeProtos.MinorType.VAR16CHAR_VALUE:
                hll.offer(in.readText().toString());
                break;
              default:
                work.obj = null;
                throw new org.apache.drill.common.exceptions.DrillRuntimeException(
                    String.format("Failure evaluating ndv: Unexpected type %s", type));
            }
            break;
          default:
            work.obj = null;
            throw new org.apache.drill.common.exceptions.DrillRuntimeException(
                String.format("Failure evaluating ndv: Unexpected type %s", type));
        }
      }
    }

    @Override
    public void output() {
      if (work.obj != null) {
        com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
            (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(contextInfo.getHllMemoryLimit());
    }
  }*/

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class BitAvgWidthFunction implements DrillAggFunc {
    @Param BitHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 1;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value * 8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBitAvgWidthFunction implements DrillAggFunc {
    @Param NullableBitHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 1;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value * 8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntAvgWidthFunction implements DrillAggFunc {
    @Param IntHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Integer.SIZE;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value * 8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntAvgWidthFunction implements DrillAggFunc {
    @Param NullableIntHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Integer.SIZE;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value * 8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class BigIntAvgWidthFunction implements DrillAggFunc {
    @Param BigIntHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Long.SIZE;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value * 8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBigIntAvgWidthFunction implements DrillAggFunc {
    @Param NullableBigIntHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Long.SIZE;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value * 8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal9AvgWidthFunction implements DrillAggFunc {
    @Param Decimal9Holder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Integer.SIZE;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value * 8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal9AvgWidthFunction implements DrillAggFunc {
    @Param NullableDecimal9Holder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Integer.SIZE;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value * 8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal18AvgWidthFunction implements DrillAggFunc {
    @Param Decimal18Holder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Long.SIZE;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value * 8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal18AvgWidthFunction implements DrillAggFunc {
    @Param NullableDecimal18Holder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Long.SIZE;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value * 8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal28DenseAvgWidthFunction implements DrillAggFunc {
    @Param Decimal28DenseHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 12;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal28DenseAvgWidthFunction implements DrillAggFunc {
    @Param NullableDecimal28DenseHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 12;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal28SparseAvgWidthFunction implements DrillAggFunc {
    @Param Decimal28SparseHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 12;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal28SparseAvgWidthFunction implements DrillAggFunc {
    @Param NullableDecimal28SparseHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 12;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal38DenseAvgWidthFunction implements DrillAggFunc {
    @Param Decimal38DenseHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 16;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal38DenseAvgWidthFunction implements DrillAggFunc {
    @Param NullableDecimal38DenseHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 16;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal38SparseAvgWidthFunction implements DrillAggFunc {
    @Param Decimal38SparseHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 16;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal38SparseAvgWidthFunction implements DrillAggFunc {
    @Param NullableDecimal38SparseHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 16;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Float4AvgWidthFunction implements DrillAggFunc {
    @Param Float4Holder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Float.SIZE;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value * 8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat4AvgWidthFunction implements DrillAggFunc {
    @Param NullableFloat4Holder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Float.SIZE;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value * 8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Float8AvgWidthFunction implements DrillAggFunc {
    @Param Float8Holder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Double.SIZE;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value * 8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat8AvgWidthFunction implements DrillAggFunc {
    @Param NullableFloat8Holder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Double.SIZE;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value * 8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class DateAvgWidthFunction implements DrillAggFunc {
    @Param DateHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Integer.SIZE;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value * 8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDateAvgWidthFunction implements DrillAggFunc {
    @Param NullableDateHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Integer.SIZE;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value * 8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TimeAvgWidthFunction implements DrillAggFunc {
    @Param TimeHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Integer.SIZE;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value * 8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeAvgWidthFunction implements DrillAggFunc {
    @Param NullableTimeHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Integer.SIZE;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value * 8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TimeStampAvgWidthFunction implements DrillAggFunc {
    @Param TimeStampHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Long.SIZE;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value * 8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeStampAvgWidthFunction implements DrillAggFunc {
    @Param NullableTimeStampHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Long.SIZE;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value * 8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntervalAvgWidthFunction implements DrillAggFunc {
    @Param IntervalHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 12;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntervalAvgWidthFunction implements DrillAggFunc {
    @Param NullableIntervalHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 12;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntervalDayAvgWidthFunction implements DrillAggFunc {
    @Param IntervalDayHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 12;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntervalDayAvgWidthFunction implements DrillAggFunc {
    @Param NullableIntervalDayHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 12;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntervalYearAvgWidthFunction implements DrillAggFunc {
    @Param IntervalYearHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 12;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntervalYearAvgWidthFunction implements DrillAggFunc {
    @Param NullableIntervalYearHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 12;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class VarCharAvgWidthFunction implements DrillAggFunc {
    @Param VarCharHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
              in.start, in.end, in.buffer).getBytes().length;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarCharAvgWidthFunction implements DrillAggFunc {
    @Param NullableVarCharHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
                in.start, in.end, in.buffer).getBytes().length;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Var16CharAvgWidthFunction implements DrillAggFunc {
    @Param Var16CharHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF16(
              in.start, in.end, in.buffer).getBytes().length;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVar16CharAvgWidthFunction implements DrillAggFunc {
    @Param NullableVar16CharHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF16(
                in.start, in.end, in.buffer).getBytes().length;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class VarBinaryAvgWidthFunction implements DrillAggFunc {
    @Param VarBinaryHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
              in.start, in.end, in.buffer).getBytes().length;
      count.value++;
    }

    @Override
    public void output() {
      out.value = totWidth.value/((double)count.value);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
    }
  }

  @FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarBinaryAvgWidthFunction implements DrillAggFunc {
    @Param NullableVarBinaryHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
                in.start, in.end, in.buffer).getBytes().length;
        nonNullCount.value += 1;
      }
      count.value++;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }
  /*@FunctionTemplate(name = "avg_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class AvgColWidth implements DrillAggFunc {
    @Param FieldReader in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder count;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      count = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      int mode = in.getType().getMode().getNumber();
      int type = in.getType().getMinorType().getNumber();

      switch (mode) {
        case org.apache.drill.common.types.TypeProtos.DataMode.OPTIONAL_VALUE:
          if (!in.isSet()) {
            break;
          }
          // fall through //
        case org.apache.drill.common.types.TypeProtos.DataMode.REQUIRED_VALUE:
          switch (type) {
            case org.apache.drill.common.types.TypeProtos.MinorType.BIT_VALUE:
              totWidth.value += 0;                          // Ignore 1/8 byte to simplify
              break;
            case org.apache.drill.common.types.TypeProtos.MinorType.DECIMAL9_VALUE:
              totWidth.value += (Integer.SIZE/8);
              break;
            case org.apache.drill.common.types.TypeProtos.MinorType.DECIMAL18_VALUE:
              totWidth.value += (Long.SIZE/8);
              break;
            case org.apache.drill.common.types.TypeProtos.MinorType.DECIMAL28DENSE_VALUE:
            case org.apache.drill.common.types.TypeProtos.MinorType.DECIMAL28SPARSE_VALUE:
              totWidth.value += 12;
              break;
            case org.apache.drill.common.types.TypeProtos.MinorType.DECIMAL38DENSE_VALUE:
            case org.apache.drill.common.types.TypeProtos.MinorType.DECIMAL38SPARSE_VALUE:
              totWidth.value += 16;
              break;
            case org.apache.drill.common.types.TypeProtos.MinorType.FLOAT4_VALUE:
              totWidth.value += (Float.SIZE/8);
              break;
            case org.apache.drill.common.types.TypeProtos.MinorType.FLOAT8_VALUE:
              totWidth.value += (Double.SIZE/8);
              break;
            case org.apache.drill.common.types.TypeProtos.MinorType.INT_VALUE:
              totWidth.value += (Integer.SIZE/8);
              break;
            case org.apache.drill.common.types.TypeProtos.MinorType.BIGINT_VALUE:
              totWidth.value += (Long.SIZE/8);
              break;
            case org.apache.drill.common.types.TypeProtos.MinorType.DATE_VALUE:
            case org.apache.drill.common.types.TypeProtos.MinorType.TIME_VALUE:
              totWidth.value += (Integer.SIZE/8);
              break;
            case org.apache.drill.common.types.TypeProtos.MinorType.TIMETZ_VALUE:
            case org.apache.drill.common.types.TypeProtos.MinorType.TIMESTAMP_VALUE:
            case org.apache.drill.common.types.TypeProtos.MinorType.TIMESTAMPTZ_VALUE:
              totWidth.value += (Long.SIZE/8);
              break;
            case org.apache.drill.common.types.TypeProtos.MinorType.INTERVAL_VALUE:
            case org.apache.drill.common.types.TypeProtos.MinorType.INTERVALDAY_VALUE:
            case org.apache.drill.common.types.TypeProtos.MinorType.INTERVALYEAR_VALUE:
              totWidth.value += 12;                          // Fixed length 12 byte array
              break;
            case org.apache.drill.common.types.TypeProtos.MinorType.VARBINARY_VALUE:
              totWidth.value += in.readByteArray().length;
              break;
            case org.apache.drill.common.types.TypeProtos.MinorType.VARCHAR_VALUE:
            case org.apache.drill.common.types.TypeProtos.MinorType.VAR16CHAR_VALUE:
              totWidth.value += in.readText().getLength();
              break;
            default:
              throw new org.apache.drill.common.exceptions.DrillRuntimeException(
                  String.format("Failure evaluating avg_width: Unexpected type %s", type));
          }
          break;
        default:
        throw new org.apache.drill.common.exceptions.DrillRuntimeException(
            String.format("Failure evaluating avg_width: Unexpected type %s", type));
      }
      count.value++;
      nonNullCount.value = 1;
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/((double)count.value);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      count.value = 0;
      nonNullCount.value = 0;
    }
  }*/
  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class BitSumWidthFunction implements DrillAggFunc {
    @Param BitHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 1;
    }

    @Override
    public void output() {
      out.value = totWidth.value/(8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBitSumWidthFunction implements DrillAggFunc {
    @Param NullableBitHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/(8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntSumWidthFunction implements DrillAggFunc {
    @Param IntHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Integer.SIZE;
    }

    @Override
    public void output() {
      out.value = totWidth.value/(8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntSumWidthFunction implements DrillAggFunc {
    @Param NullableIntHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Integer.SIZE;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/(8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class BigIntSumWidthFunction implements DrillAggFunc {
    @Param BigIntHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Long.SIZE;
    }

    @Override
    public void output() {
      out.value = totWidth.value/(8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableBigIntSumWidthFunction implements DrillAggFunc {
    @Param NullableBigIntHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Long.SIZE;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/(8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal9SumWidthFunction implements DrillAggFunc {
    @Param Decimal9Holder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Integer.SIZE;
    }

    @Override
    public void output() {
      out.value = totWidth.value/(8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal9SumWidthFunction implements DrillAggFunc {
    @Param NullableDecimal9Holder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Integer.SIZE;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/(8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal18SumWidthFunction implements DrillAggFunc {
    @Param Decimal18Holder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Long.SIZE;
    }

    @Override
    public void output() {
      out.value = totWidth.value/(8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal18SumWidthFunction implements DrillAggFunc {
    @Param NullableDecimal18Holder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Long.SIZE;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/(8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal28DenseSumWidthFunction implements DrillAggFunc {
    @Param Decimal28DenseHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 12;
    }

    @Override
    public void output() {
      out.value = totWidth.value;
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal28DenseSumWidthFunction implements DrillAggFunc {
    @Param NullableDecimal28DenseHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 12;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value;
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal28SparseSumWidthFunction implements DrillAggFunc {
    @Param Decimal28SparseHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 12;
    }

    @Override
    public void output() {
      out.value = totWidth.value;
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal28SparseSumWidthFunction implements DrillAggFunc {
    @Param NullableDecimal28SparseHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 12;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value;
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal38DenseSumWidthFunction implements DrillAggFunc {
    @Param Decimal38DenseHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 16;
    }

    @Override
    public void output() {
      out.value = totWidth.value;
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal38DenseSumWidthFunction implements DrillAggFunc {
    @Param NullableDecimal38DenseHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 16;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value;
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Decimal38SparseSumWidthFunction implements DrillAggFunc {
    @Param Decimal38SparseHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 16;
    }

    @Override
    public void output() {
      out.value = totWidth.value;
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimal38SparseSumWidthFunction implements DrillAggFunc {
    @Param NullableDecimal38SparseHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 16;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value;
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Float4SumWidthFunction implements DrillAggFunc {
    @Param Float4Holder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Float.SIZE;
    }

    @Override
    public void output() {
      out.value = totWidth.value/(8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat4SumWidthFunction implements DrillAggFunc {
    @Param NullableFloat4Holder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Float.SIZE;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/(8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Float8SumWidthFunction implements DrillAggFunc {
    @Param Float8Holder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Double.SIZE;
    }

    @Override
    public void output() {
      out.value = totWidth.value/(8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableFloat8SumWidthFunction implements DrillAggFunc {
    @Param NullableFloat8Holder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Double.SIZE;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/(8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class DateSumWidthFunction implements DrillAggFunc {
    @Param DateHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Integer.SIZE;
    }

    @Override
    public void output() {
      out.value = totWidth.value/(8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDateSumWidthFunction implements DrillAggFunc {
    @Param NullableDateHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Integer.SIZE;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/(8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TimeSumWidthFunction implements DrillAggFunc {
    @Param TimeHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Integer.SIZE;
    }

    @Override
    public void output() {
      out.value = totWidth.value/(8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeSumWidthFunction implements DrillAggFunc {
    @Param NullableTimeHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Integer.SIZE;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/(8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TimeStampSumWidthFunction implements DrillAggFunc {
    @Param TimeStampHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += Long.SIZE;
    }

    @Override
    public void output() {
      out.value = totWidth.value/(8.0);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableTimeStampSumWidthFunction implements DrillAggFunc {
    @Param NullableTimeStampHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += Long.SIZE;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value/(8.0);
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntervalSumWidthFunction implements DrillAggFunc {
    @Param IntervalHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 12;
    }

    @Override
    public void output() {
      out.value = totWidth.value;
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntervalSumWidthFunction implements DrillAggFunc {
    @Param NullableIntervalHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 12;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value;
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntervalDaySumWidthFunction implements DrillAggFunc {
    @Param IntervalDayHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 12;
    }

    @Override
    public void output() {
      out.value = totWidth.value;
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntervalDaySumWidthFunction implements DrillAggFunc {
    @Param NullableIntervalDayHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 12;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value;
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class IntervalYearSumWidthFunction implements DrillAggFunc {
    @Param IntervalYearHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += 12;
    }

    @Override
    public void output() {
      out.value = totWidth.value;
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableIntervalYearSumWidthFunction implements DrillAggFunc {
    @Param NullableIntervalYearHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += 12;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value;
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class VarCharSumWidthFunction implements DrillAggFunc {
    @Param VarCharHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
              in.start, in.end, in.buffer).getBytes().length;
    }

    @Override
    public void output() {
      out.value = totWidth.value;
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarCharSumWidthFunction implements DrillAggFunc {
    @Param NullableVarCharHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
                in.start, in.end, in.buffer).getBytes().length;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value;
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class Var16CharSumWidthFunction implements DrillAggFunc {
    @Param Var16CharHolder in;
    @Workspace BigIntHolder totWidth;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF16(
              in.start, in.end, in.buffer).getBytes().length;
    }

    @Override
    public void output() {
      out.value = totWidth.value;
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVar16CharSumWidthFunction implements DrillAggFunc {
    @Param NullableVar16CharHolder in;
    @Workspace BigIntHolder totWidth;
    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF16(
                in.start, in.end, in.buffer).getBytes().length;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value;
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }
  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class VarBinarySumWidthFunction implements DrillAggFunc {
    @Param VarBinaryHolder in;
    @Workspace BigIntHolder totWidth;

    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
    }

    @Override
    public void add() {
      totWidth.value += org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
              in.start, in.end, in.buffer).getBytes().length;
    }

    @Override
    public void output() {
      out.value = totWidth.value;
      out.isSet = 1;
    }

    @Override
    public void reset() {
      totWidth.value = 0;
    }
  }

  @FunctionTemplate(name = "sum_width", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableVarBinarySumWidthFunction implements DrillAggFunc {
    @Param NullableVarBinaryHolder in;
    @Workspace BigIntHolder totWidth;

    @Workspace BigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    @Override
    public void setup() {
      totWidth = new BigIntHolder();
      nonNullCount = new BigIntHolder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        totWidth.value += org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
                in.start, in.end, in.buffer).getBytes().length;
        nonNullCount.value += 1;
      }
    }

    @Override
    public void output() {
      if (nonNullCount.value > 0) {
        out.value = totWidth.value;
        out.isSet = 1;
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      totWidth.value = 0;
      nonNullCount.value = 0;
    }
  }
}