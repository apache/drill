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
package org.apache.drill.exec.store.parquet;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.drill.exec.store.parquet.metadata.MetadataBase.ColumnMetadata;
import static org.apache.drill.exec.store.parquet.metadata.MetadataBase.ParquetTableMetadataBase;
import static org.apache.drill.exec.store.parquet.metadata.Metadata_V3.ColumnTypeMetadata_v3;
import static org.apache.drill.exec.store.parquet.metadata.Metadata_V3.ParquetTableMetadata_v3;

/**
 * Holds common statistics about data in parquet group scan,
 * including information about total row count, columns counts, partition columns.
 */
public class ParquetGroupScanStatistics {

  // map from file names to maps of column name to partition value mappings
  private Map<Path, Map<SchemaPath, Object>> partitionValueMap;
  // only for partition columns : value is unique for each partition
  private Map<SchemaPath, TypeProtos.MajorType> partitionColTypeMap;
  // total number of non-null value for each column in parquet files
  private Map<SchemaPath, MutableLong> columnValueCounts;
  // total number of rows (obtained from parquet footer)
  private long rowCount;


  public ParquetGroupScanStatistics(List<RowGroupInfo> rowGroupInfos, ParquetTableMetadataBase parquetTableMetadata) {
    collect(rowGroupInfos, parquetTableMetadata);
  }

  public ParquetGroupScanStatistics(ParquetGroupScanStatistics that) {
    this.partitionValueMap = new HashMap<>(that.partitionValueMap);
    this.partitionColTypeMap = new HashMap<>(that.partitionColTypeMap);
    this.columnValueCounts = new HashMap<>(that.columnValueCounts);
    this.rowCount = that.rowCount;
  }

  public long getColumnValueCount(SchemaPath column) {
    MutableLong count = columnValueCounts.get(column);
    return count != null ? count.getValue() : 0;
  }

  public List<SchemaPath> getPartitionColumns() {
    return new ArrayList<>(partitionColTypeMap.keySet());
  }

  public TypeProtos.MajorType getTypeForColumn(SchemaPath schemaPath) {
    return partitionColTypeMap.get(schemaPath);
  }

  public long getRowCount() {
    return rowCount;
  }

  public Object getPartitionValue(Path path, SchemaPath column) {
    return partitionValueMap.get(path).get(column);
  }

  public void collect(List<RowGroupInfo> rowGroupInfos, ParquetTableMetadataBase parquetTableMetadata) {
    resetHolders();
    boolean first = true;
    for (RowGroupInfo rowGroup : rowGroupInfos) {
      long rowCount = rowGroup.getRowCount();
      for (ColumnMetadata column : rowGroup.getColumns()) {
        SchemaPath schemaPath = SchemaPath.getCompoundPath(column.getName());
        MutableLong emptyCount = new MutableLong();
        MutableLong previousCount = columnValueCounts.putIfAbsent(schemaPath, emptyCount);
        if (previousCount == null) {
          previousCount = emptyCount;
        }
        if (previousCount.longValue() != GroupScan.NO_COLUMN_STATS && column.isNumNullsSet()) {
          previousCount.add(rowCount - column.getNulls());
        } else {
          previousCount.setValue(GroupScan.NO_COLUMN_STATS);
        }
        boolean partitionColumn = checkForPartitionColumn(column, first, rowCount, parquetTableMetadata);
        if (partitionColumn) {
          Map<SchemaPath, Object> map = partitionValueMap.computeIfAbsent(rowGroup.getPath(), key -> new HashMap<>());
          Object value = map.get(schemaPath);
          Object currentValue = column.getMaxValue();
          if (value != null) {
            if (value != currentValue) {
              partitionColTypeMap.remove(schemaPath);
            }
          } else {
            // the value of a column with primitive type can not be null,
            // so checks that there are really null value and puts it to the map
            if (rowCount == column.getNulls()) {
              map.put(schemaPath, null);
            } else {
              map.put(schemaPath, currentValue);
            }
          }
        } else {
          partitionColTypeMap.remove(schemaPath);
        }
      }
      this.rowCount += rowGroup.getRowCount();
      first = false;
    }
  }

  /**
   * Re-init holders eigther during first instance creation or statistics update based on updated list of row groups.
   */
  private void resetHolders() {
    this.partitionValueMap = new HashMap<>();
    this.partitionColTypeMap = new HashMap<>();
    this.columnValueCounts = new HashMap<>();
    this.rowCount = 0;
  }

  /**
   * When reading the very first footer, any column is a potential partition column. So for the first footer, we check
   * every column to see if it is single valued, and if so, add it to the list of potential partition columns. For the
   * remaining footers, we will not find any new partition columns, but we may discover that what was previously a
   * potential partition column now no longer qualifies, so it needs to be removed from the list.
   *
   * @param columnMetadata column metadata
   * @param first if columns first appears in row group
   * @param rowCount row count
   * @param parquetTableMetadata parquet table metadata
   *
   * @return whether column is a potential partition column
   */
  private boolean checkForPartitionColumn(ColumnMetadata columnMetadata,
                                          boolean first,
                                          long rowCount,
                                          ParquetTableMetadataBase parquetTableMetadata) {
    SchemaPath schemaPath = SchemaPath.getCompoundPath(columnMetadata.getName());
    final PrimitiveType.PrimitiveTypeName primitiveType;
    final OriginalType originalType;
    int precision = 0;
    int scale = 0;
    if (parquetTableMetadata.hasColumnMetadata()) {
      // only ColumnTypeMetadata_v3 stores information about scale and precision
      if (parquetTableMetadata instanceof ParquetTableMetadata_v3) {
        ColumnTypeMetadata_v3 columnTypeInfo = ((ParquetTableMetadata_v3) parquetTableMetadata)
            .getColumnTypeInfo(columnMetadata.getName());
        scale = columnTypeInfo.scale;
        precision = columnTypeInfo.precision;
      }
      primitiveType = parquetTableMetadata.getPrimitiveType(columnMetadata.getName());
      originalType = parquetTableMetadata.getOriginalType(columnMetadata.getName());
    } else {
      primitiveType = columnMetadata.getPrimitiveType();
      originalType = columnMetadata.getOriginalType();
    }
    if (first) {
      if (hasSingleValue(columnMetadata, rowCount)) {
        partitionColTypeMap.put(schemaPath, ParquetReaderUtility.getType(primitiveType, originalType, scale, precision));
        return true;
      } else {
        return false;
      }
    } else {
      if (!partitionColTypeMap.keySet().contains(schemaPath)) {
        return false;
      } else {
        if (!hasSingleValue(columnMetadata, rowCount)) {
          partitionColTypeMap.remove(schemaPath);
          return false;
        }
        if (!ParquetReaderUtility.getType(primitiveType, originalType, scale, precision).equals(partitionColTypeMap.get(schemaPath))) {
          partitionColTypeMap.remove(schemaPath);
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Checks that the column chunk has a single value.
   * ColumnMetadata will have a non-null value iff the minValue and
   * the maxValue for the rowgroup are the same.
   *
   * @param columnChunkMetaData metadata to check
   * @param rowCount rows count in column chunk
   *
   * @return true if column has single value
   */
  private boolean hasSingleValue(ColumnMetadata columnChunkMetaData, long rowCount) {
    return (columnChunkMetaData != null) && (columnChunkMetaData.hasSingleValue(rowCount));
  }

}
