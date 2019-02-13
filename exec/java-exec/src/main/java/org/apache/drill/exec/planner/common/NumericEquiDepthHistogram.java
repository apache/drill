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

import org.apache.calcite.rex.RexNode;
import com.clearspring.analytics.stream.quantile.TDigest;

/**
 * A column specific equi-depth histogram which is meant for numeric data types
 */
@JsonTypeName("numeric-equi-depth")
public class NumericEquiDepthHistogram implements Histogram {

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
  public Double estimatedSelectivity(RexNode filter) {
    if (numRowsPerBucket >= 0) {
      return 1.0;
    } else {
      return null;
    }
  }

  /**
   * Utility method to build a Numeric Equi-Depth Histogram from a t-digest byte array
   * @param tdigest_array
   * @return An instance of NumericEquiDepthHistogram
   */
  public static NumericEquiDepthHistogram buildFromTDigest(byte[] tdigest_array,
                                                           int numBuckets,
                                                           long nonNullCount) {
    TDigest tdigest = TDigest.fromBytes(java.nio.ByteBuffer.wrap(tdigest_array));

    NumericEquiDepthHistogram histogram = new NumericEquiDepthHistogram(numBuckets);

    double q = 1.0/numBuckets;
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
