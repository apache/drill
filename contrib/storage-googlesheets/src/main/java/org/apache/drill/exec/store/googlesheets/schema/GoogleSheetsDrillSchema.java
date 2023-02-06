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

import com.google.api.services.drive.Drive;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.Sheet;
import org.apache.calcite.schema.Table;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Writer;
import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.planner.logical.ModifyTableEntry;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StorageStrategy;
import org.apache.drill.exec.store.googlesheets.GoogleSheetsInsertWriter;
import org.apache.drill.exec.store.googlesheets.GoogleSheetsScanSpec;
import org.apache.drill.exec.store.googlesheets.GoogleSheetsStoragePlugin;
import org.apache.drill.exec.store.googlesheets.GoogleSheetsStoragePluginConfig;
import org.apache.drill.exec.store.googlesheets.GoogleSheetsWriter;
import org.apache.drill.exec.store.googlesheets.utils.GoogleSheetsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class represents the actual tab within a GoogleSheets document.
 */
public class GoogleSheetsDrillSchema extends AbstractSchema {
  private static final Logger logger = LoggerFactory.getLogger(GoogleSheetsDrillSchema.class);
  private static final Pattern TAB_PATTERN = Pattern.compile("^tab\\[(\\d+)\\]$");

  private final Map<String, DynamicDrillTable> activeTables = CaseInsensitiveMap.newHashMap();
  private final List<DynamicDrillTable> tableList;

  private final GoogleSheetsStoragePlugin plugin;
  private final Sheets sheetsService;
  private final SchemaConfig schemaConfig;
  private final GoogleSheetsRootSchema parent;
  private final String fileToken;
  private final String fileName;

  private List<Sheet> tabList;

  public GoogleSheetsDrillSchema(AbstractSchema parent, String fileToken,
                                 GoogleSheetsStoragePlugin plugin,
                                 SchemaConfig schemaConfig,
                                 Sheets sheetsService, String fileName) {
    super(parent.getSchemaPath(), GoogleSheetsRootSchema.getFileTokenWithCorrectCase(((GoogleSheetsRootSchema) parent).getTokenMap(), fileToken));
    this.plugin = plugin;
    this.schemaConfig = schemaConfig;
    this.parent = (GoogleSheetsRootSchema) parent;
    this.fileToken = GoogleSheetsRootSchema.getFileTokenWithCorrectCase(((GoogleSheetsRootSchema) parent).getTokenMap(), fileToken);
    this.sheetsService = sheetsService;
    this.tableList = new ArrayList<>();
    this.fileName = fileName;
  }

  @Override
  public String getTypeName() {
    return GoogleSheetsStoragePluginConfig.NAME;
  }

  @Override
  public Table getTable(String tableName) {
    // If the tables map is empty, populate it
    if (activeTables.isEmpty() && GoogleSheetsUtils.isProbableFileToken(fileToken)) {
      populateActiveTables();
    }

    // If the user provides the index of a tab, return the table at that index.
    int tabIndex = getTabIndex(tableName);
    if (tabIndex > -1) {
      if (tabIndex > tableList.size()) {
        throw UserException.dataReadError()
          .message("Tab not found at index " + tabIndex)
          .build(logger);
      }
      return tableList.get(tabIndex);
    }

    // Otherwise, retrieve the table from the active tables list.
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

  private int getTabIndex(String tableName) {
    Matcher matcher = TAB_PATTERN.matcher(tableName);
    if (matcher.find()) {
      int tabIndex = Integer.parseInt(matcher.group(1));
      if (tabIndex < 0) {
        throw UserException.internalError()
          .message("Google Sheets tab index must be greater than zero.")
          .build(logger);
      }

      return tabIndex;
    } else {
      return -1;
    }
  }

  private DynamicDrillTable getDrillTable(String tableName) {
    logger.debug("Getting Drill Table {}", tableName);
    return activeTables.get(tableName);
  }

  @Override
  public Set<String> getTableNames() {
    return Collections.emptySet();
  }

  @Override
  public boolean isMutable() {
    return plugin.supportsWrite();
  }

  private void populateActiveTables() {
    try {
      tabList = GoogleSheetsUtils.getTabList(sheetsService, fileToken);
    } catch (IOException e) {
      throw UserException.connectionError(e)
        .message("Unable to obtain tab list for Google Sheet document " + fileToken + ". " + e.getMessage())
        .build(logger);
    }
    // Add sub schemas to list, then create tables
    for (Sheet sheet : tabList) {
      registerTable(sheet.getProperties().getTitle(),
        new DynamicDrillTable(plugin, plugin.getName(),
          new GoogleSheetsScanSpec(this.fileToken,
            (GoogleSheetsStoragePluginConfig) plugin.getConfig(),
            sheet.getProperties().getTitle(),
            plugin.getName(),
            tabList.indexOf(sheet), fileName
          )
        )
      );
    }
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
    String documentName = this.fileToken;
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

  @Override
  public ModifyTableEntry modifyTable(String tableName) {
    return child -> new GoogleSheetsInsertWriter(child, this.fileToken, tableName, schemaConfig.getUserName(), plugin);
  }

  @Override
  public void dropTable(String indexName) {
    logger.debug("Index name: {}", indexName);

    // The GoogleSheets API will not allow you to delete a tab if the file only has one tab.  In that case,
    // we delete the entire file.
    if (tabList.size() == 1) {
      Drive driveService = plugin.getDriveService(schemaConfig.getUserName());
      try {
        driveService.files().delete(fileToken);
      } catch (IOException e) {
        throw UserException.internalError(e)
            .message("Error deleting GoogleSheets file. " + e.getMessage())
            .build(logger);
      }
    }

    Sheet sheetToDrop = GoogleSheetsUtils.getSheetFromTabList(indexName, tabList);
    try {
      GoogleSheetsUtils.removeTabFromGoogleSheet(sheetsService, fileToken, sheetToDrop);
    } catch (IOException e) {
      throw UserException.internalError(e)
          .message(e.getMessage())
          .build(logger);
    }
  }


  private void registerTable(String name, DynamicDrillTable table) {
    activeTables.put(name, table);
    tableList.add(table);
  }
}
