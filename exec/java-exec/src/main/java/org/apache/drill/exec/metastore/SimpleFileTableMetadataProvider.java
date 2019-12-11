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
package org.apache.drill.exec.metastore;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.schema.SchemaProvider;
import org.apache.drill.exec.store.dfs.easy.SimpleFileTableMetadataProviderBuilder;
import org.apache.drill.metastore.metadata.SegmentMetadata;
import org.apache.drill.metastore.metadata.TableMetadataProvider;
import org.apache.drill.metastore.statistics.ColumnStatistics;
import org.apache.drill.metastore.metadata.FileMetadata;
import org.apache.drill.metastore.metadata.BaseTableMetadata;
import org.apache.drill.metastore.metadata.MetadataInfo;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.metastore.metadata.NonInterestingColumnsMetadata;
import org.apache.drill.metastore.metadata.PartitionMetadata;
import org.apache.drill.metastore.metadata.TableInfo;
import org.apache.drill.metastore.metadata.TableMetadata;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata provider which supplies only table schema and / or table statistics if available.
 */
public class SimpleFileTableMetadataProvider implements TableMetadataProvider {
  private static final Logger logger = LoggerFactory.getLogger(SimpleFileTableMetadataProvider.class);

  private final TableMetadata tableMetadata;

  private SimpleFileTableMetadataProvider(TableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  @Override
  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  @Override
  public List<SchemaPath> getPartitionColumns() {
    return null;
  }

  @Override
  public List<PartitionMetadata> getPartitionsMetadata() {
    return null;
  }

  @Override
  public List<PartitionMetadata> getPartitionMetadata(SchemaPath columnName) {
    return null;
  }

  @Override
  public Map<Path, FileMetadata> getFilesMetadataMap() {
    return null;
  }

  @Override
  public Map<Path, SegmentMetadata> getSegmentsMetadataMap() {
    return null;
  }

  @Override
  public FileMetadata getFileMetadata(Path location) {
    return null;
  }

  @Override
  public List<FileMetadata> getFilesForPartition(PartitionMetadata partition) {
    return null;
  }

  @Override
  public NonInterestingColumnsMetadata getNonInterestingColumnsMetadata() {
    return null;
  }

  @Override
  public boolean checkMetadataVersion() {
    return false;
  }

  public static class Builder implements SimpleFileTableMetadataProviderBuilder {
    private String tableName;
    private Path location;
    private long lastModifiedTime = -1L;
    private TupleMetadata schema;

    private final MetadataProviderManager metadataProviderManager;

    public Builder(MetadataProviderManager source) {
      this.metadataProviderManager = source;
    }

    @Override
    public SimpleFileTableMetadataProviderBuilder withTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    @Override
    public SimpleFileTableMetadataProviderBuilder withLocation(Path location) {
      this.location = location;
      return this;
    }

    @Override
    public SimpleFileTableMetadataProviderBuilder withLastModifiedTime(long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
      return this;
    }

    @Override
    public SimpleFileTableMetadataProviderBuilder withSchema(TupleMetadata schema) {
      this.schema = schema;
      return this;
    }

    @Override
    public TableMetadataProvider build() {
      SchemaProvider schemaProvider = metadataProviderManager.getSchemaProvider();
      TableMetadataProvider source = metadataProviderManager.getTableMetadataProvider();
      if (source == null) {
        DrillStatsTable statsProvider = metadataProviderManager.getStatsProvider();
        Map<SchemaPath, ColumnStatistics<?>> columnsStatistics = new HashMap<>();

        if (statsProvider != null) {
          if (!statsProvider.isMaterialized()) {
            statsProvider.materialize();
          }
          if (statsProvider.isMaterialized()) {
            for (SchemaPath column : statsProvider.getColumns()) {
              columnsStatistics.put(column,
                  new ColumnStatistics<>(DrillStatsTable.getEstimatedColumnStats(statsProvider, column)));
            }
          }
        }

        TupleMetadata schema = null;
        try {
          if (this.schema != null) {
            schema = this.schema;
          } else {
            schema = schemaProvider != null ? schemaProvider.read().getSchema() : null;
          }
        } catch (IOException | IllegalArgumentException e) {
          logger.debug("Unable to read schema from schema provider [{}]: {}", (tableName != null ? tableName : location), e.getMessage());
          logger.trace("Error when reading the schema", e);
        }
        MetadataInfo metadataInfo = MetadataInfo.builder().type(MetadataType.TABLE).build();
        TableMetadata tableMetadata = BaseTableMetadata.builder()
            .tableInfo(TableInfo.UNKNOWN_TABLE_INFO)
            .metadataInfo(metadataInfo)
            .location(location)
            .schema(schema)
            .columnsStatistics(columnsStatistics)
            .metadataStatistics(DrillStatsTable.getEstimatedTableStats(statsProvider))
            .lastModifiedTime(lastModifiedTime)
            .partitionKeys(Collections.emptyMap())
            .build();

        source = new SimpleFileTableMetadataProvider(tableMetadata);
        metadataProviderManager.setTableMetadataProvider(source);
      }
      return source;
    }
  }

}
