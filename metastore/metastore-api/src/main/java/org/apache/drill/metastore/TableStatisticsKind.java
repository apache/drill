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

import org.apache.drill.exec.expr.ExactStatisticsConstants;
import org.apache.drill.exec.physical.impl.statistics.Statistic;

import java.util.Collection;

/**
 * Implementation of {@link CollectableColumnStatisticsKind} which contain base
 * table statistics kinds with implemented {@code mergeStatistics()} method.
 */
public enum TableStatisticsKind implements CollectableTableStatisticsKind {
  /**
   * Table statistics kind which represents row count for the specific table.
   */
  ROW_COUNT(ExactStatisticsConstants.ROW_COUNT) {
    @Override
    public Long mergeStatistics(Collection<? extends BaseMetadata> statistics) {
      long rowCount = 0;
      for (BaseMetadata statistic : statistics) {
        Long statRowCount = getValue(statistic);
        if (statRowCount == null || statRowCount == Statistic.NO_COLUMN_STATS) {
          rowCount = Statistic.NO_COLUMN_STATS;
          break;
        } else {
          rowCount += statRowCount;
        }
      }
      return rowCount;
    }

    @Override
    public Long getValue(BaseMetadata metadata) {
      Long rowCount = (Long) metadata.getStatistic(this);
      return rowCount != null ? rowCount : Statistic.NO_COLUMN_STATS;
    }

    @Override
    public boolean isExact() {
      return true;
    }
  },

  /**
   * Table statistics kind which represents estimated row count for the specific table.
   */
  EST_ROW_COUNT(Statistic.ROWCOUNT) {
    @Override
    public Double mergeStatistics(Collection<? extends BaseMetadata> statisticsList) {
      double rowCount = 0;
      for (BaseMetadata statistics : statisticsList) {
        Double statRowCount = (Double) statistics.getStatistic(this);
        if (statRowCount != null) {
          rowCount += statRowCount;
        }
      }
      return rowCount;
    }

    @Override
    public Double getValue(BaseMetadata metadata) {
      return (Double) metadata.getStatistic(this);
    }
  },

  /**
   * Table statistics kind which represents estimated row count for the specific table.
   */
  HAS_STATISTICS("has_statistics") {
    @Override
    public Boolean mergeStatistics(Collection<? extends BaseMetadata> statisticsList) {
      for (BaseMetadata statistics : statisticsList) {
        Boolean hasStatistics = (Boolean) statistics.getStatistic(this);
        if (hasStatistics == null || !hasStatistics) {
          return false;
        }
      }
      return Boolean.TRUE;
    }

    @Override
    public Boolean getValue(BaseMetadata metadata) {
      return Boolean.TRUE.equals(metadata.getStatistic(this));
    }
  };

  private final String statisticKey;

  TableStatisticsKind(String statisticKey) {
    this.statisticKey = statisticKey;
  }

  public String getName() {
    return statisticKey;
  }

  /**
   * Returns value which corresponds to this statistic kind,
   * obtained from specified {@link BaseMetadata}.
   *
   * @param metadata the source of statistic value
   * @return value which corresponds to this statistic kind
   */
  public abstract Object getValue(BaseMetadata metadata);
}
