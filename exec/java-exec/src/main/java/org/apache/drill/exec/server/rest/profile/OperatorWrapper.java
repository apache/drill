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
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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
  private static final String UNKNOWN_OPERATOR = "UNKNOWN_OPERATOR";
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
    operatorName = operatorType == null ? UNKNOWN_OPERATOR : operatorType.toString();
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
    TableBuilder builder = new TableBuilder(OPERATOR_COLUMNS, null);

    for (ImmutablePair<OperatorProfile, Integer> ip : ops) {
      int minor = ip.getRight();
      OperatorProfile op = ip.getLeft();

      String path = new OperatorPathBuilder().setMajor(major).setMinor(minor).setOperator(op).build();
      builder.appendCell(path);
      builder.appendNanos(op.getSetupNanos());
      builder.appendNanos(op.getProcessNanos());
      builder.appendNanos(op.getWaitNanos());

      long maxBatches = Long.MIN_VALUE;
      long maxRecords = Long.MIN_VALUE;
      for (StreamProfile sp : op.getInputProfileList()) {
        maxBatches = Math.max(sp.getBatches(), maxBatches);
        maxRecords = Math.max(sp.getRecords(), maxRecords);
      }

      builder.appendFormattedInteger(maxBatches);
      builder.appendFormattedInteger(maxRecords);
      builder.appendBytes(op.getPeakLocalMemoryAllocated());
    }
    return builder.build();
  }

  public static final String[] OPERATORS_OVERVIEW_COLUMNS = {
      OverviewTblTxt.OPERATOR_ID, OverviewTblTxt.TYPE_OF_OPERATOR,
      OverviewTblTxt.AVG_SETUP_TIME, OverviewTblTxt.MAX_SETUP_TIME,
      OverviewTblTxt.AVG_PROCESS_TIME, OverviewTblTxt.MAX_PROCESS_TIME,
      OverviewTblTxt.MIN_WAIT_TIME, OverviewTblTxt.AVG_WAIT_TIME, OverviewTblTxt.MAX_WAIT_TIME,
      OverviewTblTxt.PERCENT_FRAGMENT_TIME, OverviewTblTxt.PERCENT_QUERY_TIME, OverviewTblTxt.ROWS,
      OverviewTblTxt.AVG_PEAK_MEMORY, OverviewTblTxt.MAX_PEAK_MEMORY
  };

  public static final String[] OPERATORS_OVERVIEW_COLUMNS_TOOLTIP = {
      OverviewTblTooltip.OPERATOR_ID, OverviewTblTooltip.TYPE_OF_OPERATOR,
      OverviewTblTooltip.AVG_SETUP_TIME, OverviewTblTooltip.MAX_SETUP_TIME,
      OverviewTblTooltip.AVG_PROCESS_TIME, OverviewTblTooltip.MAX_PROCESS_TIME,
      OverviewTblTooltip.MIN_WAIT_TIME, OverviewTblTooltip.AVG_WAIT_TIME, OverviewTblTooltip.MAX_WAIT_TIME,
      OverviewTblTooltip.PERCENT_FRAGMENT_TIME, OverviewTblTooltip.PERCENT_QUERY_TIME, OverviewTblTooltip.ROWS,
      OverviewTblTooltip.AVG_PEAK_MEMORY, OverviewTblTooltip.MAX_PEAK_MEMORY
  };

  //Palette to help shade operators sharing a common major fragment
  private static final String[] OPERATOR_OVERVIEW_BGCOLOR_PALETTE = {"#ffffff","#f2f2f2"};

  public void addSummary(TableBuilder tb, HashMap<String, Long> majorFragmentBusyTally, long majorFragmentBusyTallyTotal) {
    //Select background color from palette
    String opTblBgColor = OPERATOR_OVERVIEW_BGCOLOR_PALETTE[major%OPERATOR_OVERVIEW_BGCOLOR_PALETTE.length];
    String path = new OperatorPathBuilder().setMajor(major).setOperator(firstProfile).build();
    tb.appendCell(path, null, null, opTblBgColor);
    tb.appendCell(operatorName);

    //Get MajorFragment Busy+Wait Time Tally
    long majorBusyNanos = majorFragmentBusyTally.get(new OperatorPathBuilder().setMajor(major).build());

    double setupSum = 0.0;
    double processSum = 0.0;
    double waitSum = 0.0;
    double memSum = 0.0;
    long recordSum = 0L;
    for (ImmutablePair<OperatorProfile, Integer> ip : ops) {
      OperatorProfile profile = ip.getLeft();
      setupSum += profile.getSetupNanos();
      processSum += profile.getProcessNanos();
      waitSum += profile.getWaitNanos();
      memSum += profile.getPeakLocalMemoryAllocated();
      for (final StreamProfile sp : profile.getInputProfileList()) {
        recordSum += sp.getRecords();
      }
    }

    final ImmutablePair<OperatorProfile, Integer> longSetup = Collections.max(ops, Comparators.setupTime);
    tb.appendNanos(Math.round(setupSum / size));
    tb.appendNanos(longSetup.getLeft().getSetupNanos());

    final ImmutablePair<OperatorProfile, Integer> longProcess = Collections.max(ops, Comparators.processTime);
    tb.appendNanos(Math.round(processSum / size));
    tb.appendNanos(longProcess.getLeft().getProcessNanos());

    final ImmutablePair<OperatorProfile, Integer> shortWait = Collections.min(ops, Comparators.waitTime);
    final ImmutablePair<OperatorProfile, Integer> longWait = Collections.max(ops, Comparators.waitTime);
    tb.appendNanos(shortWait.getLeft().getWaitNanos());
    tb.appendNanos(Math.round(waitSum / size));
    tb.appendNanos(longWait.getLeft().getWaitNanos());

    tb.appendPercent(processSum / majorBusyNanos);
    tb.appendPercent(processSum / majorFragmentBusyTallyTotal);

    tb.appendFormattedInteger(recordSum);

    final ImmutablePair<OperatorProfile, Integer> peakMem = Collections.max(ops, Comparators.operatorPeakMemory);
    tb.appendBytes(Math.round(memSum / size));
    tb.appendBytes(peakMem.getLeft().getPeakLocalMemoryAllocated());
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
    final TableBuilder builder = new TableBuilder(metricsTableColumnNames, null);

    for (final ImmutablePair<OperatorProfile, Integer> ip : ops) {
      final OperatorProfile op = ip.getLeft();

      builder.appendCell(
          new OperatorPathBuilder()
          .setMajor(major)
          .setMinor(ip.getRight())
          .setOperator(op)
          .build());

      final Number[] values = new Number[metricNames.length];
      //Track new/Unknown Metrics
      final Set<Integer> unknownMetrics = new TreeSet<Integer>();
      for (final MetricValue metric : op.getMetricList()) {
        if (metric.getMetricId() < metricNames.length) {
          if (metric.hasLongValue()) {
            values[metric.getMetricId()] = metric.getLongValue();
          } else if (metric.hasDoubleValue()) {
            values[metric.getMetricId()] = metric.getDoubleValue();
          }
        } else {
          //Tracking unknown metric IDs
          unknownMetrics.add(metric.getMetricId());
        }
      }
      for (final Number value : values) {
        if (value != null) {
          builder.appendFormattedNumber(value);
        } else {
          builder.appendCell("");
        }
      }
    }
    return builder.build();
  }

  private class OverviewTblTxt {
    static final String OPERATOR_ID = "Operator ID";
    static final String TYPE_OF_OPERATOR = "Type";
    static final String AVG_SETUP_TIME = "Avg Setup Time";
    static final String MAX_SETUP_TIME = "Max Setup Time";
    static final String AVG_PROCESS_TIME = "Avg Process Time";
    static final String MAX_PROCESS_TIME = "Max Process Time";
    static final String MIN_WAIT_TIME = "Min Wait Time";
    static final String AVG_WAIT_TIME = "Avg Wait Time";
    static final String MAX_WAIT_TIME = "Max Wait Time";
    static final String PERCENT_FRAGMENT_TIME = "% Fragment Time";
    static final String PERCENT_QUERY_TIME = "% Query Time";
    static final String ROWS = "Rows";
    static final String AVG_PEAK_MEMORY = "Avg Peak Memory";
    static final String MAX_PEAK_MEMORY = "Max Peak Memory";
  }

  private class OverviewTblTooltip {
    static final String OPERATOR_ID = "Operator ID";
    static final String TYPE_OF_OPERATOR = "Operator Type";
    static final String AVG_SETUP_TIME = "Average time in setting up fragments";
    static final String MAX_SETUP_TIME = "Longest time a fragment took in setup";
    static final String AVG_PROCESS_TIME = "Average process time for a fragment";
    static final String MAX_PROCESS_TIME = "Longest process time of any fragment";
    static final String MIN_WAIT_TIME = "Shortest time a fragment spent in waiting";
    static final String AVG_WAIT_TIME = "Average wait time for a fragment";
    static final String MAX_WAIT_TIME = "Longest time a fragment spent in waiting";
    static final String PERCENT_FRAGMENT_TIME = "Percentage of the total fragment time that was spent on the operator";
    static final String PERCENT_QUERY_TIME = "Percentage of the total query time that was spent on the operator";
    static final String ROWS = "Rows emitted by scans, or consumed by other operators";
    static final String AVG_PEAK_MEMORY  =  "Average memory consumption by a fragment";
    static final String MAX_PEAK_MEMORY  =  "Highest memory consumption by a fragment";
  }
}

