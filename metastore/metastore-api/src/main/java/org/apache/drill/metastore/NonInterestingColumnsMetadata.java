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
import org.apache.drill.exec.record.metadata.TupleMetadata;
import java.util.Map;

/**
 * Represents a metadata for the non-interesting columns. Since the refresh command doesn't store the non-interesting
 * columns stats in the cache file, there is a need to mark column statistics of non-interesting as unknown to
 * differentiate the non-interesting columns from non-existent columns. Since the sole purpose of this class is to store
 * column statistics for non-interesting columns, some methods like getSchema, getStatistic, getColumn are not applicable
 * to NonInterestingColumnsMetadata.
 */
public class NonInterestingColumnsMetadata implements BaseMetadata {
  private final Map<SchemaPath, ColumnStatistics> columnsStatistics;

  public NonInterestingColumnsMetadata(
                           Map<SchemaPath, ColumnStatistics> columnsStatistics) {
    this.columnsStatistics = columnsStatistics;
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
  public TupleMetadata getSchema() {
    return null;
  }

  @Override
  public Object getStatistic(StatisticsKind statisticsKind) {
    return null;
  }

  @Override
  public boolean containsExactStatistics(StatisticsKind statisticsKind) {
    return false;
  }

  @Override
  public Object getStatisticsForColumn(SchemaPath columnName, StatisticsKind statisticsKind) {
    return columnsStatistics.get(columnName).getStatistic(statisticsKind);
  }

  @Override
  public ColumnMetadata getColumn(SchemaPath name) {
    return null;
  }
}
