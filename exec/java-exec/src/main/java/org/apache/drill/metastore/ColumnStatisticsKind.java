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

import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.expr.ExactStatisticsConstants;

import java.util.List;

/**
 * Implementation of {@link CollectableColumnStatisticsKind} which contain base
 * column statistics kinds with implemented {@code mergeStatistics()} method.
 */
public enum ColumnStatisticsKind implements CollectableColumnStatisticsKind {

  /**
   * Column statistics kind which represents nulls count for the specific column.
   */
  NULLS_COUNT(ExactStatisticsConstants.NULLS_COUNT) {
    @Override
    public Object mergeStatistics(List<? extends ColumnStatistics> statisticsList) {
      long nullsCount = 0;
      for (ColumnStatistics statistics : statisticsList) {
        Long statNullsCount = (Long) statistics.getStatistic(this);
        if (statNullsCount == null || statNullsCount == GroupScan.NO_COLUMN_STATS) {
          return GroupScan.NO_COLUMN_STATS;
        } else {
          nullsCount += statNullsCount;
        }
      }
      return nullsCount;
    }

    @Override
    public boolean isExact() {
      return true;
    }
  },

  /**
   * Column statistics kind which represents min value of the specific column.
   */
  MIN_VALUE(ExactStatisticsConstants.MIN_VALUE) {
    @Override
    @SuppressWarnings("unchecked")
    public Object mergeStatistics(List<? extends ColumnStatistics> statisticsList) {
      Object minValue = null;
      for (ColumnStatistics statistics : statisticsList) {
        Object statMinValue = statistics.getValueStatistic(this);
        if (statMinValue != null && (statistics.getValueComparator().compare(minValue, statMinValue) > 0 || minValue == null)) {
          minValue = statMinValue;
        }
      }
      return minValue;
    }

    @Override
    public boolean isValueStatistic() {
      return true;
    }

    @Override
    public boolean isExact() {
      return true;
    }
  },

  /**
   * Column statistics kind which represents max value of the specific column.
   */
  MAX_VALUE(ExactStatisticsConstants.MAX_VALUE) {
    @Override
    @SuppressWarnings("unchecked")
    public Object mergeStatistics(List<? extends ColumnStatistics> statisticsList) {
      Object maxValue = null;
      for (ColumnStatistics statistics : statisticsList) {
        Object statMaxValue = statistics.getValueStatistic(this);
        if (statMaxValue != null && statistics.getValueComparator().compare(maxValue, statMaxValue) < 0) {
          maxValue = statMaxValue;
        }
      }
      return maxValue;
    }

    @Override
    public boolean isValueStatistic() {
      return true;
    }

    @Override
    public boolean isExact() {
      return true;
    }
  };

  private final String statisticKey;

  ColumnStatisticsKind(String statisticKey) {
    this.statisticKey = statisticKey;
  }

  public String getName() {
    return statisticKey;
  }
}
