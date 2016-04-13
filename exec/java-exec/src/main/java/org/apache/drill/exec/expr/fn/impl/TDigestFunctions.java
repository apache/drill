/**
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
 */
package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionCostCategory;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;

import javax.inject.Inject;

public class TDigestFunctions {

  /**
   * Computes the tdigest for a column of doubles
   */
  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TDigestDoubleRequired implements DrillAggFunc {

    @Param
    private Float8Holder in;
    @Workspace
    private ObjectHolder digest;
    @Output
    private NullableVarBinaryHolder out;
    @Inject
    private DrillBuf buffer;

    public void setup() {
      digest = new ObjectHolder();
      digest.obj = com.tdunning.math.stats.TDigest.createAvlTreeDigest(100);
    }

    @Override
    public void add() {
      ((com.tdunning.math.stats.TDigest) digest.obj).add(in.value);
    }

    @Override
    public void output() {
      com.tdunning.math.stats.TDigest tdigest = ((com.tdunning.math.stats.TDigest) digest.obj);
      int size = tdigest.smallByteSize();
      buffer = buffer.reallocIfNeeded(size);
      tdigest.asSmallBytes(buffer.nioBuffer(0, size));
      out.isSet = 1;
      out.start = 0;
      out.buffer = buffer;
      out.end = size;
    }

    @Override
    public void reset() {
      digest.obj = com.tdunning.math.stats.TDigest.createAvlTreeDigest(100);
    }

  }

  /**
   * Computes the tdigest for a column of doubles
   */
  @FunctionTemplate(name = "tdigest", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TDigestDouble implements DrillAggFunc {

    @Param
    private NullableFloat8Holder in;
    @Workspace
    private ObjectHolder digest;
    @Output
    private NullableVarBinaryHolder out;
    @Inject
    private DrillBuf buffer;

    public void setup() {
      digest = new ObjectHolder();
      digest.obj = com.tdunning.math.stats.TDigest.createAvlTreeDigest(100);
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        ((com.tdunning.math.stats.TDigest) digest.obj).add(in.value);
      }
    }

    @Override
    public void output() {
      com.tdunning.math.stats.TDigest tdigest = ((com.tdunning.math.stats.TDigest) digest.obj);
      int size = tdigest.smallByteSize();
      buffer = buffer.reallocIfNeeded(size);
      tdigest.asSmallBytes(buffer.nioBuffer(0, size));
      out.isSet = 1;
      out.start = 0;
      out.buffer = buffer;
      out.end = size;
    }

    @Override
    public void reset() {
      digest.obj = com.tdunning.math.stats.TDigest.createAvlTreeDigest(100);
    }

  }

  /**
   * Merges the tdigest produced by the tdigest functions to produce a new tdigest
   */
  @FunctionTemplate(name = "tdigest_merge", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TDigestMerge implements DrillAggFunc {

    @Param
    private NullableVarBinaryHolder in;
    @Workspace
    private ObjectHolder digest;
    @Output
    private NullableVarBinaryHolder out;
    @Inject
    private DrillBuf buffer;

    public void setup() {
      digest = new ObjectHolder();
      digest.obj = com.tdunning.math.stats.TDigest.createAvlTreeDigest(100);
    }

    @Override
    public void add() {
      if (in.isSet == 1) {

        com.tdunning.math.stats.TDigest tDigest = com.tdunning.math.stats.AVLTreeDigest.fromBytes(in.buffer.nioBuffer(in.start, in.end - in.start));
        ((com.tdunning.math.stats.TDigest) digest.obj).add(tDigest);
      }
    }

    @Override
    public void output() {
      com.tdunning.math.stats.TDigest tdigest = ((com.tdunning.math.stats.TDigest) digest.obj);
      int size = tdigest.smallByteSize();
      buffer = buffer.reallocIfNeeded(size);
      tdigest.asSmallBytes(buffer.nioBuffer(0, size));
      out.isSet = 1;
      out.start = 0;
      out.buffer = buffer;
      out.end = size;
    }

    @Override
    public void reset() {
      digest.obj = com.tdunning.math.stats.TDigest.createAvlTreeDigest(100);
    }

  }

  /**
   * Computes the quantile q for input data in. This function should generally not be used in execution, as the DrillReduceAggregatesRule
   * should reduce it to use quantile(q, tdigest(in))
   */
  @FunctionTemplate(name = "quantile", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class QuantileConstant implements DrillAggFunc {

    @Param private NullableFloat8Holder q;
    @Param private NullableFloat8Holder in;
    @Workspace private NullableFloat8Holder quantile;
    @Workspace private ObjectHolder digest;
    @Output private NullableFloat8Holder out;

    public void setup() {
      digest = new ObjectHolder();
      digest.obj = com.tdunning.math.stats.TDigest.createAvlTreeDigest(100);
      quantile = new NullableFloat8Holder();
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        ((com.tdunning.math.stats.TDigest) digest.obj).add(in.value);
      }
      if (quantile.isSet == 0 && q.isSet == 1) {
        quantile.value = q.value;
        quantile.isSet = 1;
      }
    }

    @Override
    public void output() {
      com.tdunning.math.stats.TDigest tdigest = ((com.tdunning.math.stats.TDigest) digest.obj);
      out.value = tdigest.quantile(quantile.value);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      digest.obj = com.tdunning.math.stats.TDigest.createAvlTreeDigest(100);
    }

  }

  /**
   * Computes the approximate median from the tdigest
   */
  @FunctionTemplate(name = "tdigest_median", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class TDigestMedian implements DrillSimpleFunc {

    @Param VarBinaryHolder in;
    @Output Float8Holder out;

    public void setup() {
    }

    public void eval() {
      com.tdunning.math.stats.TDigest tDigest = com.tdunning.math.stats.AVLTreeDigest.fromBytes(in.buffer.nioBuffer(in.start, in.end - in.start));
      out.value = tDigest.quantile(0.5);
    }
  }

  /**
   * Computes the quantile from a tdigest
   */
  @FunctionTemplate(name = "tdigest_quantile", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class TDigestQuantile implements DrillSimpleFunc {

    @Param Float8Holder quantile;
    @Param VarBinaryHolder in;
    @Output Float8Holder out;

    public void setup() {
    }

    public void eval() {
      com.tdunning.math.stats.TDigest tDigest = com.tdunning.math.stats.AVLTreeDigest.fromBytes(in.buffer.nioBuffer(in.start, in.end - in.start));
      out.value = tDigest.quantile(quantile.value);
    }
  }

  /**
   * Computes the median of a column of data using the tdigest
   */
  @FunctionTemplate(name = "median", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE, costCategory = FunctionCostCategory.COMPLEX)
  public static class Median implements DrillAggFunc {

    @Param private NullableFloat8Holder in;
    @Workspace private ObjectHolder digest;
    @Output private NullableFloat8Holder out;

    public void setup() {
      digest = new ObjectHolder();
      digest.obj = com.tdunning.math.stats.TDigest.createAvlTreeDigest(100);
    }

    @Override
    public void add() {
      if (in.isSet == 1) {
        ((com.tdunning.math.stats.TDigest) digest.obj).add(in.value);
      }
    }

    @Override
    public void output() {
      com.tdunning.math.stats.TDigest tdigest = ((com.tdunning.math.stats.TDigest) digest.obj);
      out.value = tdigest.quantile(0.5);
      out.isSet = 1;
    }

    @Override
    public void reset() {
      digest.obj = com.tdunning.math.stats.TDigest.createAvlTreeDigest(100);
    }

  }
}
