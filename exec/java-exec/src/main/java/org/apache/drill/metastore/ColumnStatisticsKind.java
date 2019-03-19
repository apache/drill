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
import org.apache.drill.exec.physical.impl.statistics.Statistic;

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
  },

  /**
   * Column statistics kind which represents number of non-null values for the specific column.
   */
  NON_NULL_COUNT(Statistic.NNROWCOUNT) {
    @Override
    public Double mergeStatistics(List<? extends ColumnStatistics> statisticsList) {
      double nonNullRowCount = 0;
      for (ColumnStatistics statistics : statisticsList) {
        Double nnRowCount = (Double) statistics.getStatistic(this);
        if (nnRowCount != null) {
          nonNullRowCount += nnRowCount;
        }
      }
      return nonNullRowCount;
    }
  },

  /**
   * Column statistics kind which represents number of distinct values for the specific column.
   */
  NVD(Statistic.NDV) {
    @Override
    public Object mergeStatistics(List<? extends ColumnStatistics> statisticsList) {
      throw new UnsupportedOperationException("Cannot merge statistics for NDV");
    }
  },

  /**
   * Column statistics kind which is the width of the specific column.
   */
  AVG_WIDTH(Statistic.AVG_WIDTH) {
    @Override
    public Object mergeStatistics(List<? extends ColumnStatistics> statisticsList) {
      throw new UnsupportedOperationException("Cannot merge statistics for avg_width");
    }
  },

  /**
   * Column statistics kind which is the histogram of the specific column.
   */
  HISTOGRAM("histogram") {
    @Override
    public Object mergeStatistics(List<? extends ColumnStatistics> statisticsList) {
      throw new UnsupportedOperationException("Cannot merge statistics for histogram");
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
