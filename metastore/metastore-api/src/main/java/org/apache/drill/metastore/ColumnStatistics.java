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

/**
 * Represents collection of statistics values for specific column.
 *
 * @param <T> type of column values
 */
public interface ColumnStatistics<T> {

  /**
   * Returns statistics value which corresponds to specified {@link StatisticsKind}.
   *
   * @param statisticsKind kind of statistics which value should be returned
   * @return statistics value
   */
  Object getStatistic(StatisticsKind statisticsKind);

  /**
   * Checks whether specified statistics kind is set in this column statistics.
   *
   * @param statisticsKind statistics kind to check
   * @return true if specified statistics kind is set
   */
  boolean containsStatistic(StatisticsKind statisticsKind);

  /**
   * Checks whether specified statistics kind is set in this column statistics
   * and it corresponds to the exact statistics value.
   *
   * @param statisticsKind statistics kind to check
   * @return true if value which corresponds to the specified statistics kind is exact
   */
  boolean containsExactStatistics(StatisticsKind statisticsKind);

  /**
   * Returns {@link Comparator} for comparing values with the same type as column values.
   *
   * @return {@link Comparator}
   */
  Comparator<T> getValueComparator();

  /**
   * Returns statistics value associated with value type, like a min or max value etc.
   *
   * @param statisticsKind kind of statistics
   * @return statistics value for specified statistics kind
   */
  @SuppressWarnings("unchecked")
  default T getValueStatistic(StatisticsKind statisticsKind) {
    if (statisticsKind.isValueStatistic()) {
      return (T) getStatistic(statisticsKind);
    }
    return null;
  }

  /**
   * Returns new {@link ColumnStatistics} instance with overridden statistics taken from specified {@link ColumnStatistics}.
   *
   * @param statistics source of statistics to override
   * @return new {@link ColumnStatistics} instance with overridden statistics
   */
  ColumnStatistics<T> cloneWithStats(ColumnStatistics statistics);
}
