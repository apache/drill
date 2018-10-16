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
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

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

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class HllFieldReader implements DrillAggFunc {
    @Param FieldReader in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(10);
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
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(10);
    }
  }


  @FunctionTemplate(name = "ndv", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NdvVarBinary implements DrillAggFunc {
    @Param
    FieldReader in;
    @Workspace
    ObjectHolder work;
    @Output
    NullableBigIntHolder out;

    @Override
    public void setup() {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(10);
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
              case org.apache.drill.common.types.TypeProtos.MinorType.TIMESTAMP_VALUE:
              case org.apache.drill.common.types.TypeProtos.MinorType.TIME_VALUE:
              case org.apache.drill.common.types.TypeProtos.MinorType.TIMETZ_VALUE:
                hll.offer(in.readLocalDateTime());
                break;
              case org.apache.drill.common.types.TypeProtos.MinorType.VARBINARY_VALUE:
                hll.offer(in.readByteArray());
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

        out.isSet = 1;
        out.value = hll.cardinality();
      } else {
        out.isSet = 0;
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(10);
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
          throw new org.apache.drill.common.exceptions.DrillRuntimeException("Failure evaluation hll_decode", e);
        }
      }
    }
  }

}