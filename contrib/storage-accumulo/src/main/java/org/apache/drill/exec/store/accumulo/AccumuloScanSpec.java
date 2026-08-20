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
package org.apache.drill.exec.store.accumulo;

import java.util.List;
import java.util.Objects;

import org.apache.drill.exec.planner.logical.DrillTableSelection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Specification for an Accumulo table scan.
 *
 * <p>This class captures all scan parameters that may be pushed down to Accumulo,
 * including table name, row key ranges, column projections, filters, and limits.</p>
 */
public class AccumuloScanSpec implements DrillTableSelection {

  private final String tableName;
  private final byte[] startRow;
  private final byte[] stopRow;
  private final boolean startRowInclusive;
  private final boolean stopRowInclusive;
  private final List<AccumuloColumnSpec> columns;
  private final String filterExpression;
  private final Integer limit;
  private final boolean useSortedScanner;
  private final boolean sortDescending;

  @JsonCreator
  public AccumuloScanSpec(
      @JsonProperty("tableName") String tableName,
      @JsonProperty("startRow") byte[] startRow,
      @JsonProperty("stopRow") byte[] stopRow,
      @JsonProperty("startRowInclusive") Boolean startRowInclusive,
      @JsonProperty("stopRowInclusive") Boolean stopRowInclusive,
      @JsonProperty("columns") List<AccumuloColumnSpec> columns,
      @JsonProperty("filterExpression") String filterExpression,
      @JsonProperty("limit") Integer limit,
      @JsonProperty("useSortedScanner") Boolean useSortedScanner,
      @JsonProperty("sortDescending") Boolean sortDescending) {
    this.tableName = tableName;
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.startRowInclusive = startRowInclusive != null ? startRowInclusive : true;
    this.stopRowInclusive = stopRowInclusive != null ? stopRowInclusive : false;
    this.columns = columns;
    this.filterExpression = filterExpression;
    this.limit = limit;
    this.useSortedScanner = useSortedScanner != null ? useSortedScanner : false;
    this.sortDescending = sortDescending != null ? sortDescending : false;
  }

  /**
   * Simple constructor for basic table scan.
   */
  public AccumuloScanSpec(String tableName) {
    this(tableName, null, null, true, false, null, null, null, false, false);
  }

  @JsonProperty("tableName")
  public String getTableName() {
    return tableName;
  }

  @JsonProperty("startRow")
  public byte[] getStartRow() {
    return startRow;
  }

  @JsonProperty("stopRow")
  public byte[] getStopRow() {
    return stopRow;
  }

  @JsonProperty("startRowInclusive")
  public boolean isStartRowInclusive() {
    return startRowInclusive;
  }

  @JsonProperty("stopRowInclusive")
  public boolean isStopRowInclusive() {
    return stopRowInclusive;
  }

  @JsonProperty("columns")
  public List<AccumuloColumnSpec> getColumns() {
    return columns;
  }

  @JsonProperty("filterExpression")
  public String getFilterExpression() {
    return filterExpression;
  }

  @JsonProperty("limit")
  public Integer getLimit() {
    return limit;
  }

  @JsonProperty("useSortedScanner")
  public boolean isUseSortedScanner() {
    return useSortedScanner;
  }

  @JsonProperty("sortDescending")
  public boolean isSortDescending() {
    return sortDescending;
  }

  @JsonIgnore
  public boolean hasFilter() {
    return filterExpression != null && !filterExpression.isEmpty();
  }

  @JsonIgnore
  public boolean hasLimit() {
    return limit != null && limit > 0;
  }

  @JsonIgnore
  public boolean hasRowRange() {
    return startRow != null || stopRow != null;
  }

  @JsonIgnore
  @Override
  public String digest() {
    return toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AccumuloScanSpec that = (AccumuloScanSpec) o;
    return Objects.equals(tableName, that.tableName)
        && java.util.Arrays.equals(startRow, that.startRow)
        && java.util.Arrays.equals(stopRow, that.stopRow)
        && startRowInclusive == that.startRowInclusive
        && stopRowInclusive == that.stopRowInclusive
        && Objects.equals(columns, that.columns)
        && Objects.equals(filterExpression, that.filterExpression)
        && Objects.equals(limit, that.limit)
        && useSortedScanner == that.useSortedScanner
        && sortDescending == that.sortDescending;
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(tableName, startRowInclusive, stopRowInclusive,
        columns, filterExpression, limit, useSortedScanner, sortDescending);
    result = 31 * result + java.util.Arrays.hashCode(startRow);
    result = 31 * result + java.util.Arrays.hashCode(stopRow);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("AccumuloScanSpec{");
    sb.append("tableName='").append(tableName).append('\'');
    if (hasRowRange()) {
      sb.append(", hasRowRange=true");
    }
    if (hasFilter()) {
      sb.append(", filterExpression='").append(filterExpression).append('\'');
    }
    if (hasLimit()) {
      sb.append(", limit=").append(limit);
    }
    if (useSortedScanner) {
      sb.append(", useSortedScanner=true");
    }
    if (sortDescending) {
      sb.append(", sortDescending=true");
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Returns a new AccumuloScanSpec with the filter expression set.
   */
  public AccumuloScanSpec withFilter(String filterExpression) {
    return new AccumuloScanSpec(tableName, startRow, stopRow, startRowInclusive,
        stopRowInclusive, columns, filterExpression, limit, useSortedScanner, sortDescending);
  }

  /**
   * Returns a new AccumuloScanSpec with the limit set.
   */
  public AccumuloScanSpec withLimit(Integer limit) {
    return new AccumuloScanSpec(tableName, startRow, stopRow, startRowInclusive,
        stopRowInclusive, columns, filterExpression, limit, useSortedScanner, sortDescending);
  }

  /**
   * Returns a new AccumuloScanSpec with sorted scanner mode.
   */
  public AccumuloScanSpec withSortedScanner(boolean useSortedScanner) {
    return new AccumuloScanSpec(tableName, startRow, stopRow, startRowInclusive,
        stopRowInclusive, columns, filterExpression, limit, useSortedScanner, sortDescending);
  }

  /**
   * Returns a new AccumuloScanSpec with sort order (ascending or descending).
   */
  public AccumuloScanSpec withSortOrder(boolean descending) {
    return new AccumuloScanSpec(tableName, startRow, stopRow, startRowInclusive,
        stopRowInclusive, columns, filterExpression, limit, true, descending);
  }

  /**
   * Returns a new AccumuloScanSpec with column projection.
   */
  public AccumuloScanSpec withColumns(List<AccumuloColumnSpec> columns) {
    return new AccumuloScanSpec(tableName, startRow, stopRow, startRowInclusive,
        stopRowInclusive, columns, filterExpression, limit, useSortedScanner, sortDescending);
  }

  /**
   * Specification for a column to scan from Accumulo.
   */
  public static class AccumuloColumnSpec {
    private final String columnFamily;
    private final String columnQualifier;
    private final String drillColumnName;

    @JsonCreator
    public AccumuloColumnSpec(
        @JsonProperty("columnFamily") String columnFamily,
        @JsonProperty("columnQualifier") String columnQualifier,
        @JsonProperty("drillColumnName") String drillColumnName) {
      this.columnFamily = columnFamily;
      this.columnQualifier = columnQualifier;
      this.drillColumnName = drillColumnName;
    }

    @JsonProperty("columnFamily")
    public String getColumnFamily() {
      return columnFamily;
    }

    @JsonProperty("columnQualifier")
    public String getColumnQualifier() {
      return columnQualifier;
    }

    @JsonProperty("drillColumnName")
    public String getDrillColumnName() {
      return drillColumnName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AccumuloColumnSpec that = (AccumuloColumnSpec) o;
      return Objects.equals(columnFamily, that.columnFamily)
          && Objects.equals(columnQualifier, that.columnQualifier)
          && Objects.equals(drillColumnName, that.drillColumnName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(columnFamily, columnQualifier, drillColumnName);
    }
  }
}
