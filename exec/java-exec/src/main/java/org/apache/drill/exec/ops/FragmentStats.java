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
package org.apache.drill.exec.ops;

import java.util.List;

import org.apache.drill.exec.metrics.SingleThreadNestedCounter;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.UserBitShared.MinorFragmentProfile;
import org.apache.drill.exec.work.fragment.FragmentExecutor;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.hive12.common.collect.Lists;

public class FragmentStats {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentStats.class);

  private final static String METRIC_TIMER_FRAGMENT_TIME = MetricRegistry.name(FragmentExecutor.class,
      "completionTimes");
  private final static String METRIC_BATCHES_COMPLETED = MetricRegistry
      .name(FragmentExecutor.class, "batchesCompleted");
  private final static String METRIC_RECORDS_COMPLETED = MetricRegistry
      .name(FragmentExecutor.class, "recordsCompleted");
  private final static String METRIC_DATA_PROCESSED = MetricRegistry.name(FragmentExecutor.class, "dataProcessed");



  private final MetricRegistry metrics;

  private List<OperatorStats> operators = Lists.newArrayList();

  public final SingleThreadNestedCounter batchesCompleted;
  public final SingleThreadNestedCounter recordsCompleted;
  public final SingleThreadNestedCounter dataProcessed;
  public final Timer fragmentTime;
  private final long startTime;

  public FragmentStats(MetricRegistry metrics) {
    this.metrics = metrics;
    this.startTime = System.currentTimeMillis();
    this.fragmentTime = metrics.timer(METRIC_TIMER_FRAGMENT_TIME);
    this.batchesCompleted = new SingleThreadNestedCounter(metrics, METRIC_BATCHES_COMPLETED);
    this.recordsCompleted = new SingleThreadNestedCounter(metrics, METRIC_RECORDS_COMPLETED);
    this.dataProcessed = new SingleThreadNestedCounter(metrics, METRIC_DATA_PROCESSED);
  }

  public void addMetricsToStatus(FragmentStatus.Builder stats) {
    stats.setBatchesCompleted(batchesCompleted.get());
    stats.setDataProcessed(dataProcessed.get());
    stats.setRecordsCompleted(recordsCompleted.get());

    MinorFragmentProfile.Builder prfB = MinorFragmentProfile.newBuilder();
    prfB.setStartTime(startTime);
    prfB.setEndTime(System.currentTimeMillis());

    for(OperatorStats o : operators){
      prfB.addOperatorProfile(o.getProfile());
    }

    stats.setProfile(prfB);
  }

  public OperatorStats getOperatorStats(OpProfileDef profileDef){
    OperatorStats stats = new OperatorStats(profileDef);
    operators.add(stats);
    return stats;
  }

}
