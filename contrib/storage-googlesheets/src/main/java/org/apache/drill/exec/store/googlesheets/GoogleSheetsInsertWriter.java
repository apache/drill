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

package org.apache.drill.exec.store.googlesheets;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.store.StoragePluginRegistry;


public class GoogleSheetsInsertWriter extends GoogleSheetsWriter {

  public static final String OPERATOR_TYPE = "GOOGLESHEETS_INSERT_WRITER";

  @JsonCreator
  public GoogleSheetsInsertWriter(
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("sheetName") String sheetName,
      @JsonProperty("name") String name,
      @JsonProperty("storage") StoragePluginConfig storageConfig,
      @JsonProperty("queryUser") String queryUser,
      @JacksonInject StoragePluginRegistry engineRegistry) {
    super(child, sheetName, name, storageConfig, queryUser, engineRegistry);
  }

  public GoogleSheetsInsertWriter(PhysicalOperator child, String sheetName, String name, String queryUser, GoogleSheetsStoragePlugin plugin) {
    super(child, sheetName, name, queryUser, plugin);
  }

  @Override
  public String getOperatorType() {
    return OPERATOR_TYPE;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new GoogleSheetsInsertWriter(child, getSheetName(), getTableName(), getQueryUser(), getPlugin());
  }
}
