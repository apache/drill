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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaPathUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Base implementation of {@link TableMetadata} interface which corresponds to file system tables.
 */
public class FileTableMetadata implements TableMetadata {
  private final String tableName;
  private final Path location;
  private final TupleMetadata schema;
  private final Map<SchemaPath, ColumnStatistics> columnsStatistics;
  private final Map<String, Object> tableStatistics;
  private final Map<String, StatisticsKind> statisticsKinds;
  private final long lastModifiedTime;
  private final String owner;
  private final Set<String> partitionKeys;

  public FileTableMetadata(String tableName,
                           Path location,
                           TupleMetadata schema,
                           Map<SchemaPath, ColumnStatistics> columnsStatistics,
                           Map<StatisticsKind, Object> tableStatistics,
                           long lastModifiedTime,
                           String owner,
                           Set<String> partitionKeys) {
    this.tableName = tableName;
    this.location = location;
    this.schema = schema;
    this.columnsStatistics = columnsStatistics;
    this.tableStatistics = new HashMap<>();
    this.statisticsKinds = new HashMap<>();
    tableStatistics.forEach((statisticsKind, value) -> {
      this.tableStatistics.put(statisticsKind.getName(), value);
      this.statisticsKinds.put(statisticsKind.getName(), statisticsKind);
    });
    this.lastModifiedTime = lastModifiedTime;
    this.owner = owner;
    this.partitionKeys = partitionKeys;
  }

  @Override
  public Object getStatisticsForColumn(SchemaPath columnName, StatisticsKind statisticsKind) {
    return columnsStatistics.get(columnName).getStatistic(statisticsKind);
  }

  @Override
  public ColumnStatistics getColumnStatistics(SchemaPath columnName) {
    return columnsStatistics.get(columnName);
  }

  @Override
  public Object getStatistic(StatisticsKind statisticsKind) {
    return tableStatistics.get(statisticsKind.getName());
  }

  @Override
  public boolean containsExactStatistics(StatisticsKind statisticsKind) {
    return statisticsKinds.get(statisticsKind.getName()).isExact();
  }

  @Override
  public ColumnMetadata getColumn(SchemaPath name) {
    return SchemaPathUtils.getColumnMetadata(name, schema);
  }

  @Override
  public TupleMetadata getSchema() {
    return schema;
  }

  public boolean isPartitionColumn(String fieldName) {
    return partitionKeys.contains(fieldName);
  }

  boolean isPartitioned() {
    return !partitionKeys.isEmpty();
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public Path getLocation() {
    return location;
  }

  @Override
  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  @Override
  public String getOwner() {
    return owner;
  }

  @Override
  public Map<SchemaPath, ColumnStatistics> getColumnsStatistics() {
    return columnsStatistics;
  }

  @Override
  public FileTableMetadata cloneWithStats(Map<SchemaPath, ColumnStatistics> columnStatistics, Map<StatisticsKind, Object> tableStatistics) {
    Map<StatisticsKind, Object> mergedTableStatistics = new HashMap<>();
    this.tableStatistics.forEach((statisticsName, value) -> {
      StatisticsKind statisticsKind = statisticsKinds.get(statisticsName);
      Object statisticsValue = this.tableStatistics.get(statisticsName);
      mergedTableStatistics.put(statisticsKind, statisticsValue);
    });

    tableStatistics.forEach((statisticsKind, statisticsValue) -> {
      if (statisticsValue != null &&
        (statisticsKind.isExact() || !statisticsKinds.get(statisticsKind.getName()).isExact())) {
        // overrides statistics value for the case when new statistics is exact or existing was estimated one
        mergedTableStatistics.put(statisticsKind, statisticsValue);
      }
    });

    Map<SchemaPath, ColumnStatistics> newColumnsStatistics = new HashMap<>(this.columnsStatistics);
    for (Map.Entry<SchemaPath, ColumnStatistics> columnStatisticEntry : this.columnsStatistics.entrySet()) {
      SchemaPath columnName = columnStatisticEntry.getKey();
      newColumnsStatistics.put(columnName, columnStatisticEntry.getValue().cloneWithStats(columnStatistics.get(columnName)));
    }

    return new FileTableMetadata(tableName, location, schema, newColumnsStatistics, mergedTableStatistics, lastModifiedTime, owner, partitionKeys);
  }
}
