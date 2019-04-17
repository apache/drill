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
package org.apache.drill.metastore;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Base implementation of {@link ColumnStatistics} which is not bound
 * to the specific list of column statistic kinds.
 *
 * @param <T> type of column values
 */
public class ColumnStatisticsImpl<T> implements ColumnStatistics<T> {

  private final Map<String, Object> statistics;
  private final Map<String, StatisticsKind> statisticsKinds;
  private final Comparator<T> valueComparator;

  public ColumnStatisticsImpl(Map<StatisticsKind, Object> statistics, Comparator<T> valueComparator) {
    this.statistics = new HashMap<>();
    this.statisticsKinds = new HashMap<>();
    statistics.forEach((statisticsKind, value) -> {
      this.statistics.put(statisticsKind.getName(), value);
      this.statisticsKinds.put(statisticsKind.getName(), statisticsKind);
    });
    this.valueComparator = valueComparator;
  }

  @Override
  public Object getStatistic(StatisticsKind statisticsKind) {
    return statistics.get(statisticsKind.getName());
  }

  @Override
  public boolean containsStatistic(StatisticsKind statisticsKind) {
    return statistics.containsKey(statisticsKind.getName());
  }

  @Override
  public boolean containsExactStatistics(StatisticsKind statisticsKind) {
    return statisticsKinds.get(statisticsKind.getName()).isExact();
  }

  @Override
  public Comparator<T> getValueComparator() {
    return valueComparator;
  }

  @Override
  public ColumnStatistics<T> cloneWithStats(ColumnStatistics statistics) {
    Map<StatisticsKind, Object> newStats = new HashMap<>();
    this.statistics.forEach((statisticsName, value) -> {
      StatisticsKind statisticsKind = statisticsKinds.get(statisticsName);
      Object statisticsValue = statistics.getStatistic(statisticsKind);
      if (statisticsValue != null &&
          (statistics.containsExactStatistics(statisticsKind) || !statisticsKind.isExact())) {
        // overrides statistics value for the case when new statistics is exact or existing was estimated one
        newStats.put(statisticsKind, statisticsValue);
      } else {
        newStats.put(statisticsKind, value);
      }
    });

    return new ColumnStatisticsImpl<>(newStats, valueComparator);
  }
}
