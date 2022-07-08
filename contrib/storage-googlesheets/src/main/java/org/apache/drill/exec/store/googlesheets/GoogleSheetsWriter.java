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

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.store.StoragePluginRegistry;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class GoogleSheetsWriter extends AbstractWriter {

  public static final String OPERATOR_TYPE = "GOOGLESHEETS_WRITER";

  private final GoogleSheetsStoragePlugin plugin;
  private final String tableName;
  private final String sheetName;
  private final String queryUser;

  @JsonCreator
  public GoogleSheetsWriter(
    @JsonProperty("child") PhysicalOperator child,
    @JsonProperty("sheetName") String sheetName,
    @JsonProperty("name") String name,
    @JsonProperty("storage") StoragePluginConfig storageConfig,
    @JsonProperty("queryUser") String queryUser,
    @JacksonInject StoragePluginRegistry engineRegistry) {
    super(child);
    this.plugin = engineRegistry.resolve(storageConfig, GoogleSheetsStoragePlugin.class);
    this.sheetName = sheetName;
    this.queryUser = queryUser;
    this.tableName = name;
  }

  public GoogleSheetsWriter(PhysicalOperator child, String sheetName, String name, String queryUser, GoogleSheetsStoragePlugin plugin) {
    super(child);
    this.tableName = name;
    this.sheetName = sheetName;
    this.queryUser = queryUser;
    this.plugin = plugin;
  }

  @Override
  public String getOperatorType() {
    return OPERATOR_TYPE;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new GoogleSheetsWriter(child, sheetName, tableName, queryUser, plugin);
  }

  public String getTableName() {
    return tableName;
  }

  public String getSheetName() {
    return sheetName;
  }

  public String getQueryUser() {
    return queryUser;
  }

  public StoragePluginConfig getStorage() {
    return plugin.getConfig();
  }

  @JsonIgnore
  public GoogleSheetsStoragePlugin getPlugin() {
    return plugin;
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("tableName", tableName)
      .field("sheetName", sheetName)
      .field("queryUser", queryUser)
      .field("storageStrategy", getStorageStrategy())
      .toString();
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (that == null || getClass() != that.getClass()) {
      return false;
    }
    GoogleSheetsWriter otherWriter  = (GoogleSheetsWriter) that;
    return Objects.equals(tableName, otherWriter.tableName) &&
      Objects.equals(sheetName, otherWriter.sheetName) &&
      Objects.equals(queryUser, otherWriter.queryUser);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, sheetName, queryUser);
  }
}
