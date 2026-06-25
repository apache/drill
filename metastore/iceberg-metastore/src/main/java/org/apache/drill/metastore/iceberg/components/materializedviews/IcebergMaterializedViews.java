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
package org.apache.drill.metastore.iceberg.components.materializedviews;

import org.apache.drill.metastore.MetastoreColumn;
import org.apache.drill.metastore.components.materializedviews.MaterializedViewMetadataUnit;
import org.apache.drill.metastore.components.materializedviews.MaterializedViews;
import org.apache.drill.metastore.components.materializedviews.MaterializedViewsMetadataTypeValidator;
import org.apache.drill.metastore.iceberg.IcebergMetastoreContext;
import org.apache.drill.metastore.iceberg.operate.ExpirationHandler;
import org.apache.drill.metastore.iceberg.operate.IcebergMetadata;
import org.apache.drill.metastore.iceberg.operate.IcebergModify;
import org.apache.drill.metastore.iceberg.operate.IcebergRead;
import org.apache.drill.metastore.iceberg.schema.IcebergTableSchema;
import org.apache.drill.metastore.iceberg.transform.Transformer;
import org.apache.drill.metastore.iceberg.write.FileWriter;
import org.apache.drill.metastore.iceberg.write.ParquetFileWriter;
import org.apache.drill.metastore.operate.Metadata;
import org.apache.drill.metastore.operate.Modify;
import org.apache.drill.metastore.operate.Read;
import org.apache.iceberg.Table;

import java.util.Arrays;
import java.util.List;

/**
 * Metastore MaterializedViews component which stores MV metadata in the corresponding Iceberg table.
 * Provides methods to read and modify materialized view metadata.
 */
public class IcebergMaterializedViews implements MaterializedViews, IcebergMetastoreContext<MaterializedViewMetadataUnit> {

  /**
   * Metastore MaterializedViews component partition keys.
   * MVs are partitioned by storage plugin, workspace, and name for efficient lookups.
   */
  private static final List<MetastoreColumn> PARTITION_KEYS = Arrays.asList(
      MetastoreColumn.STORAGE_PLUGIN,
      MetastoreColumn.WORKSPACE,
      MetastoreColumn.MV_NAME);

  public static final IcebergTableSchema SCHEMA =
      IcebergTableSchema.of(MaterializedViewMetadataUnit.class, PARTITION_KEYS);

  private final Table table;
  private final ExpirationHandler expirationHandler;

  public IcebergMaterializedViews(Table table) {
    this.table = table;
    this.expirationHandler = new ExpirationHandler(table);
  }

  public IcebergMetastoreContext<MaterializedViewMetadataUnit> context() {
    return this;
  }

  @Override
  public Metadata metadata() {
    return new IcebergMetadata(table);
  }

  @Override
  public Read<MaterializedViewMetadataUnit> read() {
    return new IcebergRead<>(MaterializedViewsMetadataTypeValidator.INSTANCE, context());
  }

  @Override
  public Modify<MaterializedViewMetadataUnit> modify() {
    return new IcebergModify<>(MaterializedViewsMetadataTypeValidator.INSTANCE, context());
  }

  @Override
  public Table table() {
    return table;
  }

  @Override
  public FileWriter fileWriter() {
    return new ParquetFileWriter(table);
  }

  @Override
  public Transformer<MaterializedViewMetadataUnit> transformer() {
    return new MaterializedViewsTransformer(context());
  }

  @Override
  public ExpirationHandler expirationHandler() {
    return expirationHandler;
  }
}
