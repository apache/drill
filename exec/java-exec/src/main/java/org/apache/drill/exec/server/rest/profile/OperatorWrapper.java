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
package org.apache.drill.exec.server.rest.profile;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.proto.UserBitShared.OperatorProfile;
import org.apache.drill.exec.proto.UserBitShared.StreamProfile;

public class OperatorWrapper {
  private final int major;
  private List<ImmutablePair<OperatorProfile, Integer>> ops;

  public OperatorWrapper(int major, List<ImmutablePair<OperatorProfile, Integer>> ops) {
    assert ops.size() > 0;
    this.major = major;
    this.ops = ops;
  }

  public String getDisplayName() {
    OperatorProfile op = ops.get(0).getLeft();
    String path = new OperatorPathBuilder().setMajor(major).setOperator(op).build();
    CoreOperatorType operatorType = CoreOperatorType.valueOf(op.getOperatorType());
    return String.format("%s - %s", path, operatorType == null ? "UKNOWN_OPERATOR" : operatorType.toString());
  }

  public String getId() {
    return String.format("operator-%d-%d", major, ops.get(0).getLeft().getOperatorId());
  }

  public String getContent() {
    final String [] columns = {"Minor Fragment", "Setup", "Process", "Wait", "Max Batches", "Max Records", "Peak Mem"};
    TableBuilder builder = new TableBuilder(columns);

    for (ImmutablePair<OperatorProfile, Integer> ip : ops) {
      int minor = ip.getRight();
      OperatorProfile op = ip.getLeft();

      String path = new OperatorPathBuilder().setMajor(major).setMinor(minor).setOperator(op).build();
      builder.appendCell(path, null);
      builder.appendNanos(op.getSetupNanos(), null);
      builder.appendNanos(op.getProcessNanos(), null);
      builder.appendNanos(op.getWaitNanos(), null);

      long maxBatches = Long.MIN_VALUE;
      long maxRecords = Long.MIN_VALUE;
      for (StreamProfile sp : op.getInputProfileList()) {
        maxBatches = Math.max(sp.getBatches(), maxBatches);
        maxRecords = Math.max(sp.getRecords(), maxRecords);
      }

      builder.appendFormattedInteger(maxBatches, null);
      builder.appendFormattedInteger(maxRecords, null);
      builder.appendBytes(op.getPeakLocalMemoryAllocated(), null);
    }
    return builder.toString();
  }

  public void addSummary(TableBuilder tb) {
    OperatorProfile op = ops.get(0).getLeft();
    String path = new OperatorPathBuilder().setMajor(major).setOperator(op).build();
    tb.appendCell(path, null);
    CoreOperatorType operatorType = CoreOperatorType.valueOf(ops.get(0).getLeft().getOperatorType());
    tb.appendCell(operatorType == null ? "UNKNOWN_OPERATOR" : operatorType.toString(), null);

    String fmt = " (%s)";

    double setupSum = 0.0;
    double processSum = 0.0;
    double waitSum = 0.0;
    double memSum = 0.0;
    for (ImmutablePair<OperatorProfile, Integer> ip : ops) {
      setupSum += ip.getLeft().getSetupNanos();
      processSum += ip.getLeft().getProcessNanos();
      waitSum += ip.getLeft().getWaitNanos();
      memSum += ip.getLeft().getPeakLocalMemoryAllocated();
    }

    final ImmutablePair<OperatorProfile, Integer> shortSetup = Collections.min(ops, Comparators.setupTimeSort);
    final ImmutablePair<OperatorProfile, Integer> longSetup = Collections.max(ops, Comparators.setupTimeSort);
    tb.appendNanos(shortSetup.getLeft().getSetupNanos(), String.format(fmt, shortSetup.getRight()));
    tb.appendNanos((long) (setupSum / ops.size()), null);
    tb.appendNanos(longSetup.getLeft().getSetupNanos(), String.format(fmt, longSetup.getRight()));

    final ImmutablePair<OperatorProfile, Integer> shortProcess = Collections.min(ops, Comparators.processTimeSort);
    final ImmutablePair<OperatorProfile, Integer> longProcess = Collections.max(ops, Comparators.processTimeSort);
    tb.appendNanos(shortProcess.getLeft().getProcessNanos(), String.format(fmt, shortProcess.getRight()));
    tb.appendNanos((long) (processSum / ops.size()), null);
    tb.appendNanos(longProcess.getLeft().getProcessNanos(), String.format(fmt, longProcess.getRight()));

    final ImmutablePair<OperatorProfile, Integer> shortWait = Collections.min(ops, Comparators.waitTimeSort);
    final ImmutablePair<OperatorProfile, Integer> longWait = Collections.max(ops, Comparators.waitTimeSort);
    tb.appendNanos(shortWait.getLeft().getWaitNanos(), String.format(fmt, shortWait.getRight()));
    tb.appendNanos((long) (waitSum / ops.size()), null);
    tb.appendNanos(longWait.getLeft().getWaitNanos(), String.format(fmt, longWait.getRight()));

    final ImmutablePair<OperatorProfile, Integer> peakMem = Collections.max(ops, Comparators.opPeakMem);
    tb.appendBytes(Math.round(memSum / ops.size()), null);
    tb.appendBytes(peakMem.getLeft().getPeakLocalMemoryAllocated(), null);
  }
}