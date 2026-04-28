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
package org.apache.drill.exec.dotdrill;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Represents a materialized view definition stored as a JSON file with
 * .materialized_view.drill extension. The actual data is stored separately
 * in Parquet format in the workspace directory.
 */
@JsonTypeName("materialized_view")
public class MaterializedView {

  /**
   * Represents the refresh status of the materialized view.
   */
  public enum RefreshStatus {
    /** The materialized view data is complete and up-to-date with its definition */
    COMPLETE,
    /** The materialized view data needs to be refreshed */
    INCOMPLETE
  }

  private final String name;
  private String sql;
  private List<View.Field> fields;

  /** Current schema when materialized view is created (not the schema to which view belongs to) */
  private List<String> workspaceSchemaPath;

  /** The relative path where the materialized data is stored (typically the view name) */
  @JsonInclude(Include.NON_NULL)
  private String dataStoragePath;

  /** Timestamp of the last successful refresh in milliseconds since epoch */
  @JsonInclude(Include.NON_NULL)
  private Long lastRefreshTime;

  /** Current refresh status of the materialized view */
  @JsonInclude(Include.NON_NULL)
  private RefreshStatus refreshStatus;

  public MaterializedView(String name, String sql, RelDataType rowType, List<String> workspaceSchemaPath) {
    this(name,
        sql,
        rowType.getFieldList().stream()
            .map(f -> new View.Field(f.getName(), f.getType()))
            .collect(Collectors.toList()),
        workspaceSchemaPath,
        name,  // data storage path defaults to view name
        System.currentTimeMillis(),
        RefreshStatus.INCOMPLETE);
  }

  @JsonCreator
  public MaterializedView(
      @JsonProperty("name") String name,
      @JsonProperty("sql") String sql,
      @JsonProperty("fields") List<View.Field> fields,
      @JsonProperty("workspaceSchemaPath") List<String> workspaceSchemaPath,
      @JsonProperty("dataStoragePath") String dataStoragePath,
      @JsonProperty("lastRefreshTime") Long lastRefreshTime,
      @JsonProperty("refreshStatus") RefreshStatus refreshStatus) {
    this.name = name;
    this.sql = sql;
    this.fields = fields;
    // for backward compatibility since now all schemas and workspaces are case insensitive and stored in lower case
    // make sure that given workspace schema path is also in lower case
    this.workspaceSchemaPath = workspaceSchemaPath == null ? Collections.emptyList() :
        workspaceSchemaPath.stream()
            .map(String::toLowerCase)
            .collect(Collectors.toList());
    this.dataStoragePath = dataStoragePath != null ? dataStoragePath : name;
    this.lastRefreshTime = lastRefreshTime;
    this.refreshStatus = refreshStatus != null ? refreshStatus : RefreshStatus.INCOMPLETE;
  }

  /**
   * If view fields are present then attempts to gather them into struct type,
   * otherwise returns a dynamic record type.
   *
   * @param factory factory for rel data types creation
   * @return struct type that describes names and types of all view fields
   */
  public RelDataType getRowType(RelDataTypeFactory factory) {
    // Delegate to View's logic for row type construction
    View tempView = new View(name, sql, fields, workspaceSchemaPath);
    return tempView.getRowType(factory);
  }

  @JsonIgnore
  public boolean isDynamic() {
    return fields == null || fields.isEmpty();
  }

  public String getName() {
    return name;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public List<View.Field> getFields() {
    return fields;
  }

  public List<String> getWorkspaceSchemaPath() {
    return workspaceSchemaPath;
  }

  public String getDataStoragePath() {
    return dataStoragePath;
  }

  public void setDataStoragePath(String dataStoragePath) {
    this.dataStoragePath = dataStoragePath;
  }

  public Long getLastRefreshTime() {
    return lastRefreshTime;
  }

  public void setLastRefreshTime(Long lastRefreshTime) {
    this.lastRefreshTime = lastRefreshTime;
  }

  public RefreshStatus getRefreshStatus() {
    return refreshStatus;
  }

  public void setRefreshStatus(RefreshStatus refreshStatus) {
    this.refreshStatus = refreshStatus;
  }

  /**
   * Marks the materialized view as successfully refreshed with the current timestamp.
   */
  public void markRefreshed() {
    this.lastRefreshTime = System.currentTimeMillis();
    this.refreshStatus = RefreshStatus.COMPLETE;
  }

  /**
   * Creates a copy of this materialized view with updated refresh information.
   *
   * @param lastRefreshTime the new refresh timestamp
   * @param refreshStatus the new refresh status
   * @return a new MaterializedView instance with updated refresh info
   */
  public MaterializedView withRefreshInfo(Long lastRefreshTime, RefreshStatus refreshStatus) {
    return new MaterializedView(
        this.name,
        this.sql,
        this.fields,
        this.workspaceSchemaPath,
        this.dataStoragePath,
        lastRefreshTime,
        refreshStatus);
  }
}
