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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.exec.planner.logical.DrillTableSelection;

import java.util.Objects;

@JsonTypeName("googlesheets-scan-spec")
public class GoogleSheetsScanSpec implements DrillTableSelection {

  private final String sheetID;
  private final GoogleSheetsStoragePluginConfig config;
  private final String tableName;
  private final int tabIndex;
  private final String pluginName;
  private final String fileName;

  public GoogleSheetsScanSpec(@JsonProperty("sheetID") String sheetID,
                              @JsonProperty("config") GoogleSheetsStoragePluginConfig config,
                              @JsonProperty("tableName") String tableName,
                              @JsonProperty("pluginName") String pluginName,
                              @JsonProperty("tabIndex") int tabIndex,
                              @JsonProperty("fileName") String fileName) {
    this.sheetID = sheetID;
    this.config = config;
    this.pluginName = pluginName;
    this.tableName = tableName;
    this.tabIndex = tabIndex;
    this.fileName = fileName;
  }

  @JsonProperty("sheetID")
  public String getSheetID(){
    return sheetID;
  }

  @JsonProperty("config")
  public GoogleSheetsStoragePluginConfig getConfig() {
    return config;
  }

  @JsonProperty("tableName")
  public String getTableName() {
    return tableName;
  }

  @JsonProperty("pluginName")
  public String getPluginName() {
    return pluginName;
  }

  @JsonProperty("tabIndex")
  public int getTabIndex() {
    return tabIndex;
  }

  @JsonProperty("fileName")
  public String getFileName() {
    return fileName;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    GoogleSheetsScanSpec other = (GoogleSheetsScanSpec) obj;
    return Objects.equals(sheetID, other.sheetID) &&
      Objects.equals(config, other.config) &&
      Objects.equals(tableName, other.tableName) &&
      Objects.equals(pluginName, other.pluginName) &&
      Objects.equals(tabIndex, other.tabIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sheetID, config, tableName, tabIndex, pluginName);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("pluginName", sheetID)
      .field("config", config)
      .field("tableName", tableName)
      .field("pluginName", pluginName)
      .field("tabIndex", tabIndex)
      .toString();
  }

  @Override
  public String digest() {
    return toString();
  }
}
