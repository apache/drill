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
package org.apache.drill.exec.planner.common;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexLiteral;
import com.tdunning.math.stats.MergingDigest;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

/**
 * A column specific equi-depth histogram which is meant for numeric data types
 */
@JsonTypeName("numeric-equi-depth")
public class NumericEquiDepthHistogram implements Histogram {

  /**
   * Use a small non-zero selectivity rather than 0 to account for the fact that
   * histogram boundaries are approximate and even if some values lie outside the
   * range, we cannot be absolutely sure
   */
  static final double SMALL_SELECTIVITY = 0.0001;

  /**
   * Use a large selectivity of 1.0 whenever we are reasonably confident that all rows
   * qualify. Even if this is off by a small fraction, it is acceptable.
   */
  static final double LARGE_SELECTIVITY = 1.0;

  // For equi-depth, all buckets will have same (approx) number of rows
  @JsonProperty("numRowsPerBucket")
  private long numRowsPerBucket;

  // An array of buckets arranged in increasing order of their start boundaries
  // Note that the buckets only maintain the start point of the bucket range.
  // End point is assumed to be the same as the start point of next bucket, although
  // when evaluating the filter selectivity we should treat the interval as [start, end)
  // i.e closed on the start and open on the end
  @JsonProperty("buckets")
  private Double[] buckets;

  // Default constructor for deserializer
  public NumericEquiDepthHistogram() {}

  public NumericEquiDepthHistogram(int numBuckets) {
    // If numBuckets = N, we are keeping N + 1 entries since the (N+1)th bucket's
    // starting value is the MAX value for the column and it becomes the end point of the
    // Nth bucket.
    buckets = new Double[numBuckets + 1];
    for (int i = 0; i < buckets.length; i++) {
      buckets[i] = new Double(0.0);
    }
    numRowsPerBucket = -1;
  }

  public long getNumRowsPerBucket() {
    return numRowsPerBucket;
  }

  public void setNumRowsPerBucket(long numRows) {
    this.numRowsPerBucket = numRows;
  }

  public Double[] getBuckets() {
    return buckets;
  }

  @Override
  public Double estimatedSelectivity(final RexNode filter) {
    if (numRowsPerBucket >= 0) {
      // at a minimum, the histogram should have a start and end point of 1 bucket, so at least 2 entries
      Preconditions.checkArgument(buckets.length >= 2,  "Histogram has invalid number of entries");
      final int first = 0;
      final int last = buckets.length - 1;

      // number of buckets is 1 less than the total # entries in the buckets array since last
      // entry is the end point of the last bucket
      final int numBuckets = buckets.length - 1;
      final long totalRows = numBuckets * numRowsPerBucket;
      if (filter instanceof RexCall) {
        // get the operator
        SqlOperator op = ((RexCall) filter).getOperator();
        if (op.getKind() == SqlKind.GREATER_THAN ||
                op.getKind() == SqlKind.GREATER_THAN_OR_EQUAL) {
          Double value = getLiteralValue(filter);
          if (value != null) {

            // *** Handle the boundary conditions first ***

            // if value is less than or equal to the first bucket's start point then all rows qualify
            int result = value.compareTo(buckets[first]);
            if (result <= 0) {
              return LARGE_SELECTIVITY;
            }
            // if value is greater than the end point of the last bucket, then none of the rows qualify
            result = value.compareTo(buckets[last]);
            if (result > 0) {
              return SMALL_SELECTIVITY;
            } else if (result == 0) {
              if (op.getKind() == SqlKind.GREATER_THAN_OR_EQUAL) {
                // value is exactly equal to the last bucket's end point so we take the ratio 1/bucket_width
                long totalFilterRows = (long) (1 / (buckets[last] - buckets[last - 1]) * numRowsPerBucket);
                double selectivity = (double) totalFilterRows / totalRows;
                return selectivity;
              } else {
                // predicate is 'column > value' and value is equal to last bucket's endpoint, so none of
                // the rows qualify
                return SMALL_SELECTIVITY;
              }
            }

            // *** End of boundary conditions ****

            int n = getContainingBucket(value, numBuckets);
            if (n >= 0) {
              // all buckets to the right of containing bucket will be fully covered
              int coveredBuckets = (last) - (n + 1);
              long coveredRows = numRowsPerBucket * coveredBuckets;
              // num matching rows in the current bucket is a function of (end_point_of_bucket - value)
              long partialRows = (long) ((buckets[n + 1] - value) / (buckets[n + 1] - buckets[n]) * numRowsPerBucket);
              long totalFilterRows = partialRows + coveredRows;
              double selectivity = (double)totalFilterRows/totalRows;
              return selectivity;
            } else {
              // value does not exist in any of the buckets
              return SMALL_SELECTIVITY;
            }
          }
        } else if (op.getKind() == SqlKind.LESS_THAN ||
                op.getKind() == SqlKind.LESS_THAN_OR_EQUAL) {
          Double value = getLiteralValue(filter);
          if (value != null) {

            // *** Handle the boundary conditions first ***

            // if value is greater than the last bucket's end point then all rows qualify
            int result = value.compareTo(buckets[last]);
            if (result >= 0) {
              return LARGE_SELECTIVITY;
            }
            // if value is less than the first bucket's start point then none of the rows qualify
            result = value.compareTo(buckets[first]);
            if (result < 0) {
              return SMALL_SELECTIVITY;
            } else if (result == 0) {
              if (op.getKind() == SqlKind.LESS_THAN_OR_EQUAL) {
                // value is exactly equal to the first bucket's start point so we take the ratio 1/bucket_width
                long totalFilterRows = (long) (1 / (buckets[first + 1] - buckets[first]) * numRowsPerBucket);
                double selectivity = (double) totalFilterRows / totalRows;
                return selectivity;
              } else {
                // predicate is 'column < value' and value is equal to first bucket's start point, so none of
                // the rows qualify
                return SMALL_SELECTIVITY;
              }
            }

            // *** End of boundary conditions ****

            int n = getContainingBucket(value, numBuckets);
            if (n >= 0) {
              // all buckets to the left will be fully covered
              int coveredBuckets = n;
              long coveredRows = numRowsPerBucket * coveredBuckets;
              // num matching rows in the current bucket is a function of (value - start_point_of_bucket)
              long partialRows = (long) ((value - buckets[n]) / (buckets[n + 1] - buckets[n]) * numRowsPerBucket);
              long totalFilterRows = partialRows + coveredRows;
              double selectivity = (double)totalFilterRows / totalRows;
              return selectivity;
            } else {
              // value does not exist in any of the buckets
              return SMALL_SELECTIVITY;
            }
          }
        }
      }
    }
    return null;
  }

  private int getContainingBucket(final Double value, final int numBuckets) {
    int i = 0;
    int containing_bucket = -1;
    // check which bucket this value falls in
    for (; i <= numBuckets; i++) {
      int result = buckets[i].compareTo(value);
      if (result > 0) {
        containing_bucket = i - 1;
        break;
      } else if (result == 0) {
        containing_bucket = i;
        break;
      }
    }
    return containing_bucket;
  }

  private Double getLiteralValue(final RexNode filter) {
    Double value = null;
    List<RexNode> operands = ((RexCall) filter).getOperands();
    if (operands.size() == 2 && operands.get(1) instanceof RexLiteral) {
      RexLiteral l = ((RexLiteral) operands.get(1));

      switch (l.getTypeName()) {
        case DATE:
        case TIMESTAMP:
        case TIME:
          value = (double) ((java.util.Calendar) l.getValue()).getTimeInMillis();
          break;
        case INTEGER:
        case BIGINT:
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
        case BOOLEAN:
          value = l.getValueAs(Double.class);
          break;
        default:
          break;
      }
    }
    return value;
  }

  /**
   * Build a Numeric Equi-Depth Histogram from a t-digest byte array
   * @param tdigest_array
   * @param numBuckets
   * @param nonNullCount
   * @return An instance of NumericEquiDepthHistogram
   */
  public static NumericEquiDepthHistogram buildFromTDigest(final byte[] tdigest_array,
                                                           final int numBuckets,
                                                           final long nonNullCount) {
    MergingDigest tdigest = MergingDigest.fromBytes(java.nio.ByteBuffer.wrap(tdigest_array));

    NumericEquiDepthHistogram histogram = new NumericEquiDepthHistogram(numBuckets);

    final double q = 1.0/numBuckets;
    int i = 0;
    for (; i < numBuckets; i++) {
      // get the starting point of the i-th quantile
      double start = tdigest.quantile(q * i);
      histogram.buckets[i] = start;
    }
    // for the N-th bucket, the end point corresponds to the 1.0 quantile but we don't keep the end
    // points; only the start point, so this is stored as the start point of the (N+1)th bucket
    histogram.buckets[i] = tdigest.quantile(1.0);

    // Each bucket stores approx equal number of rows.  Here, we take into consideration the nonNullCount
    // supplied since the stats may have been collected with sampling.  Sampling of 20% means only 20% of the
    // tuples will be stored in the t-digest.  However, the overall stats such as totalRowCount, nonNullCount and
    // NDV would have already been extrapolated up from the sample. So, we take the max of the t-digest size and
    // the supplied nonNullCount. Why non-null ? Because only non-null values are stored in the t-digest.
    long numRowsPerBucket = (Math.max(tdigest.size(), nonNullCount))/numBuckets;
    histogram.setNumRowsPerBucket(numRowsPerBucket);

    return histogram;
  }

}
