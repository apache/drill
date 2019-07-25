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
package org.apache.drill.metastore.iceberg.components.tables;

import org.apache.drill.metastore.components.tables.Tables;
import org.apache.drill.metastore.iceberg.operate.ExpirationHandler;
import org.apache.drill.metastore.operate.Metadata;
import org.apache.drill.metastore.operate.Modify;
import org.apache.drill.metastore.operate.Read;
import org.apache.drill.metastore.components.tables.TableMetadataUnit;
import org.apache.drill.metastore.iceberg.MetastoreContext;
import org.apache.drill.metastore.iceberg.operate.IcebergMetadata;
import org.apache.drill.metastore.iceberg.schema.IcebergTableSchema;
import org.apache.drill.metastore.iceberg.operate.IcebergModify;
import org.apache.drill.metastore.iceberg.operate.IcebergRead;
import org.apache.drill.metastore.iceberg.transform.Transformer;
import org.apache.drill.metastore.iceberg.write.FileWriter;
import org.apache.drill.metastore.iceberg.write.ParquetFileWriter;
import org.apache.iceberg.Table;

import java.util.Arrays;
import java.util.List;

/**
 * Metastore Tables component which stores tables metadata in the corresponding Iceberg table.
 * Provides methods to read and modify tables metadata.
 */
public class IcebergTables implements Tables, MetastoreContext<TableMetadataUnit> {

  public static final String STORAGE_PLUGIN = "storagePlugin";
  public static final String WORKSPACE = "workspace";
  public static final String TABLE_NAME = "tableName";
  public static final String METADATA_KEY = "metadataKey";

  /**
   * Metastore Tables component partition keys, order of partitioning will be determined based
   * on order in {@link List} holder.
   */
  private static final List<String> PARTITION_KEYS = Arrays.asList(STORAGE_PLUGIN, WORKSPACE, TABLE_NAME, METADATA_KEY);

  public static IcebergTableSchema SCHEMA = IcebergTableSchema.of(TableMetadataUnit.class, PARTITION_KEYS);

  private final Table table;
  private final ExpirationHandler expirationHandler;

  public IcebergTables(Table table, ExpirationHandler expirationHandler) {
    this.table = table;
    this.expirationHandler = expirationHandler;
  }

  public MetastoreContext<TableMetadataUnit> context() {
    return this;
  }

  @Override
  public Metadata metadata() {
    return new IcebergMetadata(table);
  }

  @Override
  public Read<TableMetadataUnit> read() {
    return new IcebergRead<>(context());
  }

  @Override
  public Modify<TableMetadataUnit> modify() {
    return new IcebergModify<>(context());
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
  public Transformer<TableMetadataUnit> transformer() {
    return new TablesTransformer(context());
  }

  @Override
  public ExpirationHandler expirationHandler() {
    return expirationHandler;
  }
}
