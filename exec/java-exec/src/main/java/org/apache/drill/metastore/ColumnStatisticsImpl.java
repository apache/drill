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

  private Map<String, Object> statistics;
  private Comparator<T> valueComparator;

  public ColumnStatisticsImpl(Map<String, Object> statistics, Comparator<T> valueComparator) {
    this.statistics = statistics;
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
  public Comparator<T> getValueComparator() {
    return valueComparator;
  }

  @Override
  public ColumnStatistics<T> cloneWithStats(ColumnStatistics statistics) {
    Map<String, Object> newStats = new HashMap<>(this.statistics);
    for (String statisticsKey : this.statistics.keySet()) {
      Object statisticsValue = statistics.getStatistic(() -> statisticsKey);
      if (statisticsValue != null) {
        newStats.put(statisticsKey, statisticsValue);
      }
    }

    return new ColumnStatisticsImpl<>(newStats, valueComparator);
  }
}
