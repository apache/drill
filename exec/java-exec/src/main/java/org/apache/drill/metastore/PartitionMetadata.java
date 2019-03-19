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
 * Represents a metadata for the table part, which corresponds to the specific partition key.
 */
public class PartitionMetadata implements BaseMetadata {
  private final SchemaPath column;
  private final TupleMetadata schema;
  private final Map<SchemaPath, ColumnStatistics> columnsStatistics;
  private final Map<String, Object> partitionStatistics;
  private final Map<String, StatisticsKind> statisticsKinds;
  private final Set<Path> location;
  private final String tableName;
  private final long lastModifiedTime;

  public PartitionMetadata(SchemaPath column,
                           TupleMetadata schema,
                           Map<SchemaPath, ColumnStatistics> columnsStatistics,
                           Map<StatisticsKind, Object> partitionStatistics,
                           Set<Path> location,
                           String tableName,
                           long lastModifiedTime) {
    this.column = column;
    this.schema = schema;
    this.columnsStatistics = columnsStatistics;
    this.partitionStatistics = new HashMap<>();
    this.statisticsKinds = new HashMap<>();
    partitionStatistics.forEach((statisticsKind, value) -> {
      this.partitionStatistics.put(statisticsKind.getName(), value);
      this.statisticsKinds.put(statisticsKind.getName(), statisticsKind);
    });
    this.location = location;
    this.tableName = tableName;
    this.lastModifiedTime = lastModifiedTime;
  }

  @Override
  public ColumnMetadata getColumn(SchemaPath name) {
    return SchemaPathUtils.getColumnMetadata(name, schema);
  }

  @Override
  public TupleMetadata getSchema() {
    return schema;
  }

  @Override
  public Map<SchemaPath, ColumnStatistics> getColumnsStatistics() {
    return columnsStatistics;
  }

  @Override
  public ColumnStatistics getColumnStatistics(SchemaPath columnName) {
    return columnsStatistics.get(columnName);
  }

  @Override
  public Object getStatistic(StatisticsKind statisticsKind) {
    return partitionStatistics.get(statisticsKind.getName());
  }

  @Override
  public boolean containsExactStatistics(StatisticsKind statisticsKind) {
    return statisticsKinds.get(statisticsKind.getName()).isExact();
  }

  @Override
  public Object getStatisticsForColumn(SchemaPath columnName, StatisticsKind statisticsKind) {
    return columnsStatistics.get(columnName).getStatistic(statisticsKind);
  }

  /**
   * It allows to obtain the column path for this partition
   *
   * @return column path
   */
  public SchemaPath getColumn() {
    return column;
  }

  /**
   * File locations for this partition
   *
   * @return file locations
   */
  public Set<Path> getLocations() {
    return location;
  }

  /**
   * Table name of this partition
   *
   * @return table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * It allows to check the time, when any files were modified. It is in Unix Timestamp
   *
   * @return last modified time of files
   */
  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

}
