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

import org.apache.commons.collections.Buffer;
import org.apache.drill.exec.proto.UserBitShared.MetricValue;
import org.apache.drill.exec.proto.UserBitShared.OperatorProfile;
import org.apache.drill.exec.proto.UserBitShared.StreamProfile;

import com.carrotsearch.hppc.IntDoubleOpenHashMap;
import com.carrotsearch.hppc.IntLongOpenHashMap;

public class OperatorStats {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorStats.class);

  protected final int operatorId;
  protected final int operatorType;

  private IntLongOpenHashMap longMetrics = new IntLongOpenHashMap();
  private IntDoubleOpenHashMap doubleMetrics = new IntDoubleOpenHashMap();

  public long[] recordsReceivedByInput;
  public long[] batchesReceivedByInput;
  private long[] schemaCountByInput;


  private boolean inProcessing = false;
  private boolean inSetup = false;

  protected long processingNanos;
  protected long setupNanos;

  private long processingMark;
  private long setupMark;

  private long schemas;

  public OperatorStats(OpProfileDef def){
    this(def.getOperatorId(), def.getOperatorType(), def.getIncomingCount());
  }

  private OperatorStats(int operatorId, int operatorType, int inputCount) {
    super();
    this.operatorId = operatorId;
    this.operatorType = operatorType;
    this.recordsReceivedByInput = new long[inputCount];
    this.batchesReceivedByInput = new long[inputCount];
    this.schemaCountByInput = new long[inputCount];
  }

  public void startSetup() {
    assert !inSetup;
    stopProcessing();
    inSetup = true;
    setupMark = System.nanoTime();
  }

  public void stopSetup() {
    assert inSetup;
    startProcessing();
    setupNanos += System.nanoTime() - setupMark;
    inSetup = false;
  }

  public void startProcessing() {
    assert !inProcessing;
    processingMark = System.nanoTime();
    inProcessing = true;
  }

  public void stopProcessing() {
    assert inProcessing;
    processingNanos += System.nanoTime() - processingMark;
    inProcessing = false;
  }

  public void batchReceived(int inputIndex, long records, boolean newSchema) {
    recordsReceivedByInput[inputIndex] += records;
    batchesReceivedByInput[inputIndex]++;
    if(newSchema){
      schemaCountByInput[inputIndex]++;
    }
  }

  public OperatorProfile getProfile() {
    final OperatorProfile.Builder b = OperatorProfile //
        .newBuilder() //
        .setOperatorType(operatorType) //
        .setOperatorId(operatorId) //
        .setSetupNanos(setupNanos) //
        .setProcessNanos(processingNanos);

    addAllMetrics(b);

    return b.build();
  }

  public void addAllMetrics(OperatorProfile.Builder builder) {
    addStreamProfile(builder);
    addLongMetrics(builder);
    addDoubleMetrics(builder);
  }

  public void addStreamProfile(OperatorProfile.Builder builder) {
    for(int i = 0; i < recordsReceivedByInput.length; i++){
      builder.addInputProfile(StreamProfile.newBuilder().setBatches(batchesReceivedByInput[i]).setRecords(recordsReceivedByInput[i]).setSchemas(this.schemaCountByInput[i]));
    }
  }

  public void addLongMetrics(OperatorProfile.Builder builder) {
    for(int i =0; i < longMetrics.allocated.length; i++){
      if(longMetrics.allocated[i]){
        builder.addMetric(MetricValue.newBuilder().setMetricId(longMetrics.keys[i]).setLongValue(longMetrics.values[i]));
      }
    }
  }

  public void addDoubleMetrics(OperatorProfile.Builder builder) {
    for(int i =0; i < doubleMetrics.allocated.length; i++){
      if(doubleMetrics.allocated[i]){
        builder.addMetric(MetricValue.newBuilder().setMetricId(doubleMetrics.keys[i]).setDoubleValue(doubleMetrics.values[i]));
      }
    }
  }

  public void addLongStat(MetricDef metric, long value){
    longMetrics.putOrAdd(metric.metricId(), value, value);
  }

  public void addDoubleStat(MetricDef metric, double value){
    doubleMetrics.putOrAdd(metric.metricId(), value, value);
  }

}
