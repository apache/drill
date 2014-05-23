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

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.partitionsender.PartitionStatsBatch;
import org.apache.drill.exec.physical.impl.partitionsender.PartitionSenderStats;
import org.apache.drill.exec.proto.UserBitShared;

import java.util.List;

public class SenderStats extends OperatorStats {

  long minReceiverRecordCount = 0;
  long maxReceiverRecordCount = 0;
  int nSenders = 0;

  public SenderStats(PhysicalOperator operator) {
    super(new OpProfileDef(operator.getOperatorId(), operator.getOperatorType(), OperatorContext.getChildCount(operator)));
  }

  public void updatePartitionStats(List<? extends PartitionStatsBatch> outgoing) {

    for (PartitionStatsBatch o : outgoing) {
      long totalRecords = o.getTotalRecords();

      minReceiverRecordCount = Math.min(minReceiverRecordCount, totalRecords);
      maxReceiverRecordCount = Math.max(maxReceiverRecordCount, totalRecords);
    }
    nSenders = outgoing.size();
  }

  @Override
  public UserBitShared.OperatorProfile getProfile() {
    final UserBitShared.OperatorProfile.Builder b = UserBitShared.OperatorProfile //
        .newBuilder() //
        .setOperatorType(operatorType) //
        .setOperatorId(operatorId) //
        .setSetupNanos(setupNanos) //
        .setProcessNanos(processingNanos);

    addAllMetrics(b);

    return b.build();

  }

  public void addAllMetrics(UserBitShared.OperatorProfile.Builder b) {
    super.addAllMetrics(b);

    b.addMetric(UserBitShared.MetricValue.newBuilder().setLongValue(minReceiverRecordCount).
        setMetricId(PartitionSenderStats.MIN_RECORDS.metricId()));
    b.addMetric(UserBitShared.MetricValue.newBuilder().setLongValue(maxReceiverRecordCount).
        setMetricId(PartitionSenderStats.MAX_RECORDS.metricId()));
    b.addMetric(UserBitShared.MetricValue.newBuilder().setLongValue(nSenders)
        .setMetricId(PartitionSenderStats.N_SENDERS.metricId()));
  }
}
