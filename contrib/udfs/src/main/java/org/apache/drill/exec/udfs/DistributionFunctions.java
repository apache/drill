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

    @Output
    IntHolder bucket;

    @Override
    public void setup() {
      double max = MaxRangeValueHolder.value;
      double min = MinRangeValueHolder.value;
      int bucketCount = bucketCountHolder.value;
      binWidth = (max - min) / bucketCount;
    }

    @Override
    public void eval() {
      // There is probably a more elegant way of doing this...
      double binFloor = MinRangeValueHolder.value;
      double binCeiling = binFloor + binWidth;

      for (int i = 1; i <= bucketCountHolder.value; i++) {
        if (inputValue.value <= binCeiling && inputValue.value > binFloor) {
           bucket.value = i;
           break;
        } else {
          binFloor = binCeiling;
          binCeiling = binWidth * (i + 1);
        }
      }
    }
  }

  @FunctionTemplate(
      names = {"pearson_correlation","pearsonCorrelation"},
      scope = FunctionScope.POINT_AGGREGATE,
      nulls = NullHandling.INTERNAL
  )
  public static class PCorrelation implements DrillAggFunc {

    @Param
    Float8Holder xInput;

    @Param
    Float8Holder yInput;

    @Workspace
    IntHolder numValues;

    @Workspace
    Float8Holder xSum;

    @Workspace
    Float8Holder ySum;

    @Workspace
    Float8Holder xSqSum;

    @Workspace
    Float8Holder ySqSum;

    @Workspace
    Float8Holder xySum;

    @Output
    Float8Holder output;

    public void setup() {
      // Initialize values
      numValues.value = 0;
      xSum.value = 0.0;
      ySum.value = 0.0;
      xSqSum.value = 0.0;
      ySqSum.value = 0.0;
      xySum.value = 0.0;
    }

    public void reset() {
      // Initialize values
      numValues.value = 0;
      xSum.value = 0.0;
      ySum.value = 0.0;
      xSqSum.value = 0.0;
      ySqSum.value = 0.0;
      xySum.value = 0.0;
    }

    public void add() {
      numValues.value = numValues.value + 1;
      xSum.value = xSum.value + xInput.value;
      ySum.value = ySum.value + yInput.value;
      xSqSum.value = xSqSum.value + (xInput.value * xInput.value);
      ySqSum.value = ySqSum.value + (yInput.value * yInput.value);
      xySum.value = xySum.value + (xInput.value * yInput.value);
    }

    public void output() {
      int n = numValues.value;
      double x = xSum.value;
      double y = ySum.value;
      double x2 = xSqSum.value;
      double y2 = ySqSum.value;
      double xy = xySum.value;
      output.value = (n * xy - x * y) / (java.lang.Math.sqrt(n * x2 - x * x) * java.lang.Math.sqrt(n * y2 - y * y));
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
          //Tie...
        }

        prevXValue.value = xInput.value;
        prevYValue.value = yInput.value;
        n.value = n.value + 1;

      } else if(n.value == 0){
        prevXValue.value = xValue;
        prevYValue.value = yValue;
        n.value = n.value + 1;
      }
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
}
