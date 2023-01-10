/*
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

package org.apache.drill.exec.udfs;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;

public class DistributionFunctions {

  @FunctionTemplate(names = {"width_bucket", "widthBucket"},
      scope = FunctionScope.SIMPLE,
      nulls = NullHandling.NULL_IF_NULL)
  public static class WidthBucketFunction implements DrillSimpleFunc {

    @Param
    Float8Holder inputValue;

    @Param
    Float8Holder MinRangeValueHolder;

    @Param
    Float8Holder MaxRangeValueHolder;

    @Param
    IntHolder bucketCountHolder;

    @Workspace
    double binWidth;

    @Workspace
    int bucketCount;

    @Output
    IntHolder bucket;

    @Override
    public void setup() {
      double max = MaxRangeValueHolder.value;
      double min = MinRangeValueHolder.value;
      bucketCount = bucketCountHolder.value;
      binWidth = (max - min) / bucketCount;
    }

    @Override
    public void eval() {
      if (inputValue.value < MinRangeValueHolder.value) {
        bucket.value = 0;
      } else if (inputValue.value > MaxRangeValueHolder.value) {
        bucket.value = bucketCount + 1;
      } else {
        bucket.value = (int) (1 + (inputValue.value - MinRangeValueHolder.value) / binWidth);
      }
    }
  }

  @FunctionTemplate(
      names = {"kendall_correlation","kendallCorrelation", "kendallTau", "kendall_tau"},
      scope = FunctionScope.POINT_AGGREGATE,
      nulls = NullHandling.INTERNAL
  )
  public static class KendallTauFunction implements DrillAggFunc {
    @Param
    Float8Holder xInput;

    @Param
    Float8Holder yInput;

    @Workspace
    Float8Holder prevXValue;

    @Workspace
    Float8Holder prevYValue;

    @Workspace
    IntHolder concordantPairs;

    @Workspace
    IntHolder discordantPairs;

    @Workspace
    IntHolder n;

    @Output
    Float8Holder tau;

    @Override
    public void add() {
      double xValue = xInput.value;
      double yValue = yInput.value;

      if (n.value > 0) {
        if ((xValue > prevXValue.value && yValue > prevYValue.value) || (xValue < prevXValue.value && yValue < prevYValue.value)) {
          concordantPairs.value = concordantPairs.value + 1;
        } else if ((xValue > prevXValue.value && yValue < prevYValue.value) || (xValue < prevXValue.value && yValue > prevYValue.value)) {
          discordantPairs.value = discordantPairs.value + 1;
        } else {
          // Tie...
        }
        n.value = n.value + 1;

      } else if (n.value == 0){
        n.value = n.value + 1;
      }
      prevXValue.value = xValue;
      prevYValue.value = yValue;

    }

    @Override
    public void setup() {
    }

    @Override
    public void reset() {
      prevXValue.value = 0;
      prevYValue.value = 0;
      concordantPairs.value = 0;
      discordantPairs.value = 0;
      n.value = 0;
    }

    @Override
    public void output() {
      double result = 0.0;
      result = (concordantPairs.value - discordantPairs.value) / (0.5 * n.value * (n.value - 1));
      tau.value = result;
    }
  }

  @FunctionTemplate(names = {"regr_slope", "regrSlope"},
      scope = FunctionScope.POINT_AGGREGATE,
      nulls = NullHandling.INTERNAL)
  public static class RegrSlopeFunction implements DrillAggFunc {

    @Param
    Float8Holder xInput;

    @Param
    Float8Holder yInput;

    @Workspace
    Float8Holder sum_x;

    @Workspace
    Float8Holder sum_y;

    @Workspace
    Float8Holder avg_x;

    @Workspace
    Float8Holder avg_y;

    @Workspace
    Float8Holder diff_x;

    @Workspace
    Float8Holder diff_y;

    @Workspace
    Float8Holder ss_x;

    @Workspace
    Float8Holder ss_xy;

    @Workspace
    IntHolder recordCount;

    @Output
    Float8Holder slope;
    @Override
    public void setup() {
      recordCount.value = 0;
      sum_y.value = 0;
      sum_x.value = 0;
      avg_x.value = 0;
      avg_y.value = 0;
      diff_x.value = 0;
      diff_y.value = 0;
      ss_x.value = 0;
      ss_xy.value = 0;
    }

    @Override
    public void add() {
      recordCount.value += 1;
      sum_x.value += xInput.value;
      avg_x.value = sum_x.value / recordCount.value;
      diff_x.value = avg_x.value - xInput.value;
      ss_x.value = (diff_x.value * diff_x.value) + ss_x.value;

      // Now compute the sum of squares for the y
      sum_y.value = sum_y.value + yInput.value;
      avg_y.value = sum_y.value / recordCount.value;
      diff_y.value = avg_y.value - yInput.value;

      ss_xy.value = (diff_x.value * diff_y.value) + ss_xy.value;
    }

    @Override
    public void output() {
      slope.value = ss_xy.value / ss_x.value;
    }

    @Override
    public void reset() {
      recordCount.value = 0;
      sum_y.value = 0;
      sum_x.value = 0;
      avg_x.value = 0;
      avg_y.value = 0;
      diff_x.value = 0;
      diff_y.value = 0;
      ss_x.value = 0;
      ss_xy.value = 0;
    }
  }

  @FunctionTemplate(names = {"regr_intercept", "regrIntercept"},
      scope = FunctionScope.POINT_AGGREGATE,
      nulls = NullHandling.INTERNAL)
  public static class RegrInterceptFunction implements DrillAggFunc {

    @Param
    Float8Holder xInput;

    @Param
    Float8Holder yInput;

    @Workspace
    Float8Holder sum_x;

    @Workspace
    Float8Holder sum_y;

    @Workspace
    Float8Holder avg_x;

    @Workspace
    Float8Holder avg_y;

    @Workspace
    Float8Holder diff_x;

    @Workspace
    Float8Holder diff_y;

    @Workspace
    Float8Holder ss_x;

    @Workspace
    Float8Holder ss_xy;

    @Workspace
    IntHolder recordCount;

    @Output
    Float8Holder intercept;
    @Override
    public void setup() {
      recordCount.value = 0;
      sum_y.value = 0;
      sum_x.value = 0;
      avg_x.value = 0;
      avg_y.value = 0;
      diff_x.value = 0;
      diff_y.value = 0;
      ss_x.value = 0;
      ss_xy.value = 0;
    }

    @Override
    public void add() {
      recordCount.value += 1;
      sum_x.value += xInput.value;
      avg_x.value = sum_x.value / recordCount.value;
      diff_x.value = avg_x.value - xInput.value;
      ss_x.value = (diff_x.value * diff_x.value) + ss_x.value;

      // Now compute the sum of squares for the y
      sum_y.value = sum_y.value + yInput.value;
      avg_y.value = sum_y.value / recordCount.value;
      diff_y.value = avg_y.value - yInput.value;

      ss_xy.value = (diff_x.value * diff_y.value) + ss_xy.value;
    }

    @Override
    public void output() {
      double slope = ss_xy.value / ss_x.value;
      intercept.value = avg_y.value - slope * avg_x.value;
    }

    @Override
    public void reset() {
      recordCount.value = 0;
      sum_y.value = 0;
      sum_x.value = 0;
      avg_x.value = 0;
      avg_y.value = 0;
      diff_x.value = 0;
      diff_y.value = 0;
      ss_x.value = 0;
      ss_xy.value = 0;
    }
  }
}
