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

package org.apache.drill.exec.store.googlesheets.schema;

import com.google.api.services.sheets.v4.model.Sheet;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Writer;
import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StorageStrategy;
import org.apache.drill.exec.store.googlesheets.GoogleSheetsScanSpec;
import org.apache.drill.exec.store.googlesheets.GoogleSheetsStoragePlugin;
import org.apache.drill.exec.store.googlesheets.GoogleSheetsStoragePluginConfig;
import org.apache.drill.exec.store.googlesheets.GoogleSheetsWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class represents the actual tab within a GoogleSheets document.
 */
public class GoogleSheetsDrillSchema extends AbstractSchema {
  private static final Logger logger = LoggerFactory.getLogger(GoogleSheetsDrillSchema.class);

  private final Map<String, DynamicDrillTable> activeTables = CaseInsensitiveMap.newHashMap();
  private final GoogleSheetsStoragePlugin plugin;

  private final SchemaConfig schemaConfig;

  public GoogleSheetsDrillSchema(AbstractSchema parent, String name,
                                 GoogleSheetsStoragePlugin plugin,
                                 List<Sheet> subSchemas, SchemaConfig schemaConfig) {
    super(parent.getSchemaPath(), name);
    this.plugin = plugin;
    this.schemaConfig = schemaConfig;

    // Add sub schemas to list, then create tables
    for (Sheet sheet : subSchemas) {
      registerTable(sheet.getProperties().getTitle(),
        new DynamicDrillTable(plugin, plugin.getName(),
        new GoogleSheetsScanSpec(
          name,
          (GoogleSheetsStoragePluginConfig) plugin.getConfig(),
          sheet.getProperties().getTitle(),
          plugin.getName(),
          subSchemas.indexOf(sheet))
        )
      );
    }
  }

  public void setHolder(SchemaPlus plusOfThis) {
    for (String s : getSubSchemaNames()) {
      GoogleSheetsDrillSchema inner = getSubSchema(s);
      SchemaPlus holder = plusOfThis.add(s, inner);
      inner.setHolder(holder);
    }
  }

  @Override
  public String getTypeName() {
    return GoogleSheetsStoragePluginConfig.NAME;
  }

  @Override
  public Table getTable(String tableName) {
    logger.debug("Getting table: {}", tableName);
    DynamicDrillTable table = activeTables.computeIfAbsent(tableName, this::getDrillTable);
    if (table != null) {
      logger.debug("Found table: {}", table.getJdbcTableType().jdbcName);
    } else {
      logger.debug("Oh no! {} not found and returning null!", tableName);
      return null;
    }
    return table;
  }

  private DynamicDrillTable getDrillTable(String tableName) {
    logger.debug("Getting Drill Table {}", tableName);
    return activeTables.get(tableName);
  }

  @Override
  public Set<String> getTableNames() {
    return activeTables.keySet();
  }

  @Override
  public GoogleSheetsDrillSchema getSubSchema(String name) {
    return null;
  }

  @Override
  public boolean isMutable() {
    return plugin.supportsWrite();
  }

  @Override
  public CreateTableEntry createNewTable(String tableName,
                                         List<String> partitionColumns,
                                         StorageStrategy storageStrategy) {
    if (! plugin.supportsWrite()) {
      throw UserException
        .dataWriteError()
        .message(plugin.getName() + " is not writable.")
        .build(logger);
    }
    String documentName = this.name;
    return new CreateTableEntry() {
      @Override
      public Writer getWriter(PhysicalOperator child) {
        return new GoogleSheetsWriter(child, documentName, tableName, schemaConfig.getUserName(), plugin);
      }

      @Override
      public List<String> getPartitionColumns() {
        return Collections.emptyList();
      }
    };
  }

  private DynamicDrillTable registerTable(String name, DynamicDrillTable table) {
    activeTables.put(name, table);
    return table;
  }
}
