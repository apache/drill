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

import org.apache.drill.exec.physical.impl.ScreenCreator;
import org.apache.drill.exec.physical.impl.SingleSenderCreator;
import org.apache.drill.exec.physical.impl.aggregate.HashAggTemplate;
import org.apache.drill.exec.physical.impl.broadcastsender.BroadcastSenderRootExec;
import org.apache.drill.exec.physical.impl.join.HashJoinBatch;
import org.apache.drill.exec.physical.impl.mergereceiver.MergingRecordBatch;
import org.apache.drill.exec.physical.impl.partitionsender.PartitionSenderRootExec;
import org.apache.drill.exec.physical.impl.unorderedreceiver.UnorderedReceiverBatch;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;

/**
 * Helper class to retrieve operator metric names.
 */
public class OperatorMetricsMapping {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorToMetricsMapping.class);

  // Mapping: operator type --> metric id --> metric name
  private static final String[][] OPERATOR_METRICS = new String[CoreOperatorType.values().length][];

  static {
    addMetricNames(CoreOperatorType.SCREEN_VALUE, ScreenCreator.ScreenRoot.Metric.class);
    addMetricNames(CoreOperatorType.SINGLE_SENDER_VALUE, SingleSenderCreator.SingleSenderRootExec.Metric.class);
    addMetricNames(CoreOperatorType.BROADCAST_SENDER_VALUE, BroadcastSenderRootExec.Metric.class);
    addMetricNames(CoreOperatorType.HASH_PARTITION_SENDER_VALUE, PartitionSenderRootExec.Metric.class);
    addMetricNames(CoreOperatorType.MERGING_RECEIVER_VALUE, MergingRecordBatch.Metric.class);
    addMetricNames(CoreOperatorType.UNORDERED_RECEIVER_VALUE, UnorderedReceiverBatch.Metric.class);
    addMetricNames(CoreOperatorType.HASH_AGGREGATE_VALUE, HashAggTemplate.Metric.class);
    addMetricNames(CoreOperatorType.HASH_JOIN_VALUE, HashJoinBatch.Metric.class);
  }

  private static void addMetricNames(final int operatorType, final Class<? extends MetricDef> metricDef) {
    final MetricDef[] enumConstants = metricDef.getEnumConstants();
    if (enumConstants != null) {
      final String[] names = new String[enumConstants.length];
      for (int i = 0; i < enumConstants.length; i++) {
        names[i] = enumConstants[i].name();
      }
      OPERATOR_METRICS[operatorType] = names;
    }
  }

  /**
   * Given an operator type and a metric id, this method returns the metric name.
   *
   * @param operatorType the operator type
   * @param metricId     metric id
   * @return metric name if metric was registered, null otherwise
   */
  public static String getMetricName(final int operatorType, final int metricId) {
    return OPERATOR_METRICS[operatorType] != null ? OPERATOR_METRICS[operatorType][metricId] : null;
  }
}
