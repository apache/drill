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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.drill.exec.ops.OperatorMetricRegistry;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.proto.UserBitShared.MetricValue;
import org.apache.drill.exec.proto.UserBitShared.OperatorProfile;
import org.apache.drill.exec.proto.UserBitShared.StreamProfile;

/**
 * Wrapper class for profiles of ALL operator instances of the same operator type within a major fragment.
 */
public class OperatorWrapper {
  private static final String format = " (%s)";

  private final int major;
  private final List<ImmutablePair<OperatorProfile, Integer>> ops; // operator profile --> minor fragment number
  private final OperatorProfile firstProfile;
  private final CoreOperatorType operatorType;
  private final String operatorName;
  private final int size;

  public OperatorWrapper(int major, List<ImmutablePair<OperatorProfile, Integer>> ops) {
    Preconditions.checkArgument(ops.size() > 0);
    this.major = major;
    firstProfile = ops.get(0).getLeft();
    operatorType = CoreOperatorType.valueOf(firstProfile.getOperatorType());
    operatorName = operatorType == null ? "UNKNOWN_OPERATOR" : operatorType.toString();
    this.ops = ops;
    size = ops.size();
  }

  public String getDisplayName() {
    final String path = new OperatorPathBuilder().setMajor(major).setOperator(firstProfile).build();
    return String.format("%s - %s", path, operatorName);
  }

  public String getId() {
    return String.format("operator-%d-%d", major, ops.get(0).getLeft().getOperatorId());
  }

  public static final String [] OPERATOR_COLUMNS = {"Minor Fragment", "Setup Time", "Process Time", "Wait Time",
    "Max Batches", "Max Records", "Peak Memory"};

  public String getContent() {
    TableBuilder builder = new TableBuilder(OPERATOR_COLUMNS);

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
    return builder.build();
  }

  public static final String[] OPERATORS_OVERVIEW_COLUMNS = {"Operator ID", "Type", "Min Setup Time", "Avg Setup Time",
    "Max Setup Time", "Min Process Time", "Avg Process Time", "Max Process Time", "Min Wait Time", "Avg Wait Time",
    "Max Wait Time", "Avg Peak Memory", "Max Peak Memory"};

  public void addSummary(TableBuilder tb) {

    String path = new OperatorPathBuilder().setMajor(major).setOperator(firstProfile).build();
    tb.appendCell(path, null);
    tb.appendCell(operatorName, null);

    double setupSum = 0.0;
    double processSum = 0.0;
    double waitSum = 0.0;
    double memSum = 0.0;
    for (ImmutablePair<OperatorProfile, Integer> ip : ops) {
      OperatorProfile profile = ip.getLeft();
      setupSum += profile.getSetupNanos();
      processSum += profile.getProcessNanos();
      waitSum += profile.getWaitNanos();
      memSum += profile.getPeakLocalMemoryAllocated();
    }

    final ImmutablePair<OperatorProfile, Integer> shortSetup = Collections.min(ops, Comparators.setupTime);
    final ImmutablePair<OperatorProfile, Integer> longSetup = Collections.max(ops, Comparators.setupTime);
    tb.appendNanos(shortSetup.getLeft().getSetupNanos(), String.format(format, shortSetup.getRight()));
    tb.appendNanos(Math.round(setupSum / size), null);
    tb.appendNanos(longSetup.getLeft().getSetupNanos(), String.format(format, longSetup.getRight()));

    final ImmutablePair<OperatorProfile, Integer> shortProcess = Collections.min(ops, Comparators.processTime);
    final ImmutablePair<OperatorProfile, Integer> longProcess = Collections.max(ops, Comparators.processTime);
    tb.appendNanos(shortProcess.getLeft().getProcessNanos(), String.format(format, shortProcess.getRight()));
    tb.appendNanos(Math.round(processSum / size), null);
    tb.appendNanos(longProcess.getLeft().getProcessNanos(), String.format(format, longProcess.getRight()));

    final ImmutablePair<OperatorProfile, Integer> shortWait = Collections.min(ops, Comparators.waitTime);
    final ImmutablePair<OperatorProfile, Integer> longWait = Collections.max(ops, Comparators.waitTime);
    tb.appendNanos(shortWait.getLeft().getWaitNanos(), String.format(format, shortWait.getRight()));
    tb.appendNanos(Math.round(waitSum / size), null);
    tb.appendNanos(longWait.getLeft().getWaitNanos(), String.format(format, longWait.getRight()));

    final ImmutablePair<OperatorProfile, Integer> peakMem = Collections.max(ops, Comparators.operatorPeakMemory);
    tb.appendBytes(Math.round(memSum / size), null);
    tb.appendBytes(peakMem.getLeft().getPeakLocalMemoryAllocated(), null);
  }

  public String getMetricsTable() {
    if (operatorType == null) {
      return "";
    }
    final String[] metricNames = OperatorMetricRegistry.getMetricNames(operatorType.getNumber());
    if (metricNames == null) {
      return "";
    }

    final String[] metricsTableColumnNames = new String[metricNames.length + 1];
    metricsTableColumnNames[0] = "Minor Fragment";
    int i = 1;
    for (final String metricName : metricNames) {
      metricsTableColumnNames[i++] = metricName;
    }
    final TableBuilder builder = new TableBuilder(metricsTableColumnNames);
    for (final ImmutablePair<OperatorProfile, Integer> ip : ops) {
      final OperatorProfile op = ip.getLeft();

      builder.appendCell(
          new OperatorPathBuilder()
              .setMajor(major)
              .setMinor(ip.getRight())
              .setOperator(op)
              .build(),
          null);

      final Number[] values = new Number[metricNames.length];
      for (final MetricValue metric : op.getMetricList()) {
        if (metric.hasLongValue()) {
          values[metric.getMetricId()] = metric.getLongValue();
        } else if (metric.hasDoubleValue()) {
          values[metric.getMetricId()] = metric.getDoubleValue();
        }
      }
      for (final Number value : values) {
        if (value != null) {
          builder.appendFormattedNumber(value, null);
        } else {
          builder.appendCell("", null);
        }
      }
    }
    return builder.build();
  }
}
