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
 * Registry of operator metrics.
 */
public class OperatorMetricRegistry {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorMetricRegistry.class);

  // Mapping: operator type --> metric id --> metric name
  private static final String[][] OPERATOR_METRICS = new String[CoreOperatorType.values().length][];

  static {
    register(CoreOperatorType.SCREEN_VALUE, ScreenCreator.ScreenRoot.Metric.class);
    register(CoreOperatorType.SINGLE_SENDER_VALUE, SingleSenderCreator.SingleSenderRootExec.Metric.class);
    register(CoreOperatorType.BROADCAST_SENDER_VALUE, BroadcastSenderRootExec.Metric.class);
    register(CoreOperatorType.HASH_PARTITION_SENDER_VALUE, PartitionSenderRootExec.Metric.class);
    register(CoreOperatorType.MERGING_RECEIVER_VALUE, MergingRecordBatch.Metric.class);
    register(CoreOperatorType.UNORDERED_RECEIVER_VALUE, UnorderedReceiverBatch.Metric.class);
    register(CoreOperatorType.HASH_AGGREGATE_VALUE, HashAggTemplate.Metric.class);
    register(CoreOperatorType.HASH_JOIN_VALUE, HashJoinBatch.Metric.class);
  }

  private static void register(final int operatorType, final Class<? extends MetricDef> metricDef) {
    // Currently registers a metric def that has enum constants
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
   * Given an operator type, this method returns an array of metric names (indexable by metric id).
   *
   * @param operatorType the operator type
   * @return metric names if operator was registered, null otherwise
   */
  public static String[] getMetricNames(final int operatorType) {
    return OPERATOR_METRICS[operatorType];
  }

  // to prevent instantiation
  private OperatorMetricRegistry() {
  }
}
