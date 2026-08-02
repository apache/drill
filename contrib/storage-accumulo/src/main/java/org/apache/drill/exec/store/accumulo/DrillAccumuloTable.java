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

import java.util.ArrayList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.accumulo.schema.ColumnDef;
import org.apache.drill.exec.store.accumulo.schema.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an Accumulo table in Drill's query planner.
 *
 * <p>This class provides the row type (schema) for Accumulo tables to Drill's
 * Calcite-based query planner. It uses the configured schema provider to
 * discover column definitions.</p>
 *
 * <p>Schema resolution follows this order:</p>
 * <ol>
 *   <li>If explicit schema is defined in the metadata table, use that</li>
 *   <li>Otherwise, use a dynamic schema with row_key and a columns map</li>
 * </ol>
 */
public class DrillAccumuloTable extends DrillTable {
  private static final Logger logger = LoggerFactory.getLogger(DrillAccumuloTable.class);

  public static final String ROW_KEY_COLUMN = "row_key";
  public static final String COLUMNS_MAP_COLUMN = "columns";

  private final AccumuloStoragePlugin plugin;
  private final AccumuloScanSpec scanSpec;
  private TableSchema tableSchema;

  public DrillAccumuloTable(
      AccumuloStoragePlugin plugin,
      String storageEngineName,
      AccumuloScanSpec scanSpec) {
    super(storageEngineName, plugin, scanSpec);
    this.plugin = plugin;
    this.scanSpec = scanSpec;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    TableSchema schema = getTableSchema();

    ArrayList<RelDataType> typeList = new ArrayList<>();
    ArrayList<String> fieldNameList = new ArrayList<>();

    // Always include row_key as first column
    fieldNameList.add(ROW_KEY_COLUMN);
    typeList.add(typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(schema.getRowKeyType().getSqlTypeName()),
        false));

    if (schema.hasExplicitColumns()) {
      // Use explicit schema from metadata table
      for (ColumnDef column : schema.getColumns()) {
        fieldNameList.add(column.getName());
        RelDataType columnType = createColumnType(typeFactory, column);
        typeList.add(typeFactory.createTypeWithNullability(columnType, column.isNullable()));
      }
      logger.debug("Using explicit schema for table '{}' with {} columns",
          scanSpec.getTableName(), schema.getColumnCount());
    } else {
      // Use dynamic schema with columns map
      fieldNameList.add(COLUMNS_MAP_COLUMN);
      typeList.add(typeFactory.createMapType(
          typeFactory.createSqlType(SqlTypeName.VARCHAR),
          typeFactory.createSqlType(SqlTypeName.ANY)));
      logger.debug("Using dynamic schema for table '{}'", scanSpec.getTableName());
    }

    return typeFactory.createStructType(typeList, fieldNameList);
  }

  /**
   * Creates a RelDataType for the given column definition.
   */
  private RelDataType createColumnType(RelDataTypeFactory typeFactory, ColumnDef column) {
    SqlTypeName sqlType = column.getSqlTypeName();

    switch (sqlType) {
      case VARCHAR:
      case CHAR:
        // Use default precision for string types
        return typeFactory.createSqlType(sqlType, 65535);
      case DECIMAL:
        // Use default precision and scale for decimal
        return typeFactory.createSqlType(sqlType, 38, 10);
      default:
        return typeFactory.createSqlType(sqlType);
    }
  }

  /**
   * Returns the table schema, loading it from the schema provider if needed.
   */
  public TableSchema getTableSchema() {
    if (tableSchema == null) {
      try {
        tableSchema = plugin.getSchemaProvider()
            .getTableSchema(plugin.getClient(), scanSpec.getTableName());
      } catch (Exception e) {
        logger.warn("Failed to load schema for table '{}', using dynamic schema",
            scanSpec.getTableName(), e);
        tableSchema = TableSchema.dynamic(scanSpec.getTableName());
      }
    }
    return tableSchema;
  }

  public AccumuloScanSpec getScanSpec() {
    return scanSpec;
  }

  public AccumuloStoragePlugin getPlugin() {
    return plugin;
  }
}
