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
import org.apache.calcite.schema.Table;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.googlesheets.GoogleSheetsStoragePlugin;
import org.apache.drill.exec.store.googlesheets.GoogleSheetsStoragePluginConfig;
import org.apache.drill.exec.store.googlesheets.utils.GoogleSheetsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GoogleSheetsRootSchema extends AbstractSchema {
  private static final Logger logger = LoggerFactory.getLogger(GoogleSheetsRootSchema.class);

  private final Map<String, DynamicDrillTable> activeTables = new HashMap<>();
  private final Map<String, GoogleSheetsDrillSchema> schemas = new HashMap<>();
  private final GoogleSheetsStoragePlugin plugin;
  private final SchemaConfig schemaConfig;
  private final Map<String, String> tokenMap;

  public GoogleSheetsRootSchema(GoogleSheetsStoragePlugin plugin, SchemaConfig schemaConfig) {
    super(Collections.emptyList(), plugin.getName());
    this.schemaConfig = schemaConfig;
    this.plugin = plugin;
    Drive driveService = plugin.getDriveService(schemaConfig.getUserName());
    try {
      this.tokenMap = GoogleSheetsUtils.getTokenToNameMap(driveService);
    } catch (IOException e) {
      throw UserException.internalError()
        .message("Could not connect to Drive service: " + e.getMessage())
        .build(logger);
    }
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return tokenMap.keySet();
  }

  @Override
  public AbstractSchema getSubSchema(String name) {
    GoogleSheetsDrillSchema schema = schemas.get(name);
    // This level here represents the actual Google document. Attempt to validate that it exists, and
    // if so, add it to the schema list.  If not, throw an exception.
    if (schema == null) {
      Sheets service = plugin.getSheetsService(schemaConfig.getUserName());
      // At this point we know we have a valid sheet because we obtained the Sheet list, so we need to
      // add the schema to the schemas list and return it.
      schema = new GoogleSheetsDrillSchema(this, name, plugin, schemaConfig, service);
      schemas.put(name, schema);
    }
    return schema;
  }

  @Override
  public Table getTable(String tableName) {
    logger.debug("Getting table in root schema: {}", tableName);
    DynamicDrillTable table = activeTables.computeIfAbsent(tableName, this::getDrillTable);
    if (table != null) {
      logger.debug("Found table: {}", table.getJdbcTableType().jdbcName);
    } else {
      logger.debug("Oh no! {} not found and returning null!", tableName);
    }
    return table;
  }

  private DynamicDrillTable getDrillTable(String tableName) {
    logger.debug("Getting Drill Table in Root schema {}", tableName);
    return activeTables.get(tableName);
  }

  @Override
  public String getTypeName() {
    return GoogleSheetsStoragePluginConfig.NAME;
  }
}
