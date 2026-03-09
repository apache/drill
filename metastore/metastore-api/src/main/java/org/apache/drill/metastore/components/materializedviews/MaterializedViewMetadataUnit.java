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
package org.apache.drill.metastore.components.materializedviews;

import org.apache.drill.metastore.MetastoreColumn;
import org.apache.drill.metastore.MetastoreFieldDefinition;
import org.apache.drill.metastore.exceptions.MetastoreException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import static org.apache.drill.metastore.metadata.MetadataType.MATERIALIZED_VIEW;

/**
 * Class that represents one row in Drill Metastore Materialized Views
 * which is a representation of materialized view metadata.
 * <p>
 * Contains information about the MV definition (name, SQL, schema),
 * storage location, and refresh status.
 */
public class MaterializedViewMetadataUnit {

  public static final Schema SCHEMA = Schema.of(MaterializedViewMetadataUnit.class, Builder.class);

  public static final MaterializedViewMetadataUnit EMPTY_UNIT = MaterializedViewMetadataUnit.builder().build();

  // Storage location identifiers
  @MetastoreFieldDefinition(column = MetastoreColumn.STORAGE_PLUGIN, scopes = {MATERIALIZED_VIEW})
  private final String storagePlugin;

  @MetastoreFieldDefinition(column = MetastoreColumn.WORKSPACE, scopes = {MATERIALIZED_VIEW})
  private final String workspace;

  // MV identification
  @MetastoreFieldDefinition(column = MetastoreColumn.MV_NAME, scopes = {MATERIALIZED_VIEW})
  private final String name;

  @MetastoreFieldDefinition(column = MetastoreColumn.OWNER, scopes = {MATERIALIZED_VIEW})
  private final String owner;

  // MV definition
  @MetastoreFieldDefinition(column = MetastoreColumn.MV_SQL, scopes = {MATERIALIZED_VIEW})
  private final String sql;

  @MetastoreFieldDefinition(column = MetastoreColumn.SCHEMA, scopes = {MATERIALIZED_VIEW})
  private final String schema;

  @MetastoreFieldDefinition(column = MetastoreColumn.MV_FIELDS, scopes = {MATERIALIZED_VIEW})
  private final String fields;

  // Data location
  @MetastoreFieldDefinition(column = MetastoreColumn.MV_DATA_LOCATION, scopes = {MATERIALIZED_VIEW})
  private final String dataLocation;

  @MetastoreFieldDefinition(column = MetastoreColumn.MV_WORKSPACE_SCHEMA_PATH, scopes = {MATERIALIZED_VIEW})
  private final List<String> workspaceSchemaPath;

  // Refresh status
  @MetastoreFieldDefinition(column = MetastoreColumn.MV_REFRESH_STATUS, scopes = {MATERIALIZED_VIEW})
  private final String refreshStatus;

  @MetastoreFieldDefinition(column = MetastoreColumn.MV_LAST_REFRESH_TIME, scopes = {MATERIALIZED_VIEW})
  private final Long lastRefreshTime;

  // Metadata
  @MetastoreFieldDefinition(column = MetastoreColumn.LAST_MODIFIED_TIME, scopes = {MATERIALIZED_VIEW})
  private final Long lastModifiedTime;

  @MetastoreFieldDefinition(column = MetastoreColumn.ADDITIONAL_METADATA, scopes = {MATERIALIZED_VIEW})
  private final String additionalMetadata;

  private MaterializedViewMetadataUnit(Builder builder) {
    this.storagePlugin = builder.storagePlugin;
    this.workspace = builder.workspace;
    this.name = builder.name;
    this.owner = builder.owner;
    this.sql = builder.sql;
    this.schema = builder.schema;
    this.fields = builder.fields;
    this.dataLocation = builder.dataLocation;
    this.workspaceSchemaPath = builder.workspaceSchemaPath;
    this.refreshStatus = builder.refreshStatus;
    this.lastRefreshTime = builder.lastRefreshTime;
    this.lastModifiedTime = builder.lastModifiedTime;
    this.additionalMetadata = builder.additionalMetadata;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String storagePlugin() {
    return storagePlugin;
  }

  public String workspace() {
    return workspace;
  }

  public String name() {
    return name;
  }

  public String owner() {
    return owner;
  }

  public String sql() {
    return sql;
  }

  public String schema() {
    return schema;
  }

  public String fields() {
    return fields;
  }

  public String dataLocation() {
    return dataLocation;
  }

  public List<String> workspaceSchemaPath() {
    return workspaceSchemaPath;
  }

  public String refreshStatus() {
    return refreshStatus;
  }

  public Long lastRefreshTime() {
    return lastRefreshTime;
  }

  public Long lastModifiedTime() {
    return lastModifiedTime;
  }

  public String additionalMetadata() {
    return additionalMetadata;
  }

  public Builder toBuilder() {
    return MaterializedViewMetadataUnit.builder()
        .storagePlugin(storagePlugin)
        .workspace(workspace)
        .name(name)
        .owner(owner)
        .sql(sql)
        .schema(schema)
        .fields(fields)
        .dataLocation(dataLocation)
        .workspaceSchemaPath(workspaceSchemaPath)
        .refreshStatus(refreshStatus)
        .lastRefreshTime(lastRefreshTime)
        .lastModifiedTime(lastModifiedTime)
        .additionalMetadata(additionalMetadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storagePlugin, workspace, name, owner, sql, schema, fields,
        dataLocation, workspaceSchemaPath, refreshStatus, lastRefreshTime,
        lastModifiedTime, additionalMetadata);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MaterializedViewMetadataUnit that = (MaterializedViewMetadataUnit) o;
    return Objects.equals(storagePlugin, that.storagePlugin)
        && Objects.equals(workspace, that.workspace)
        && Objects.equals(name, that.name)
        && Objects.equals(owner, that.owner)
        && Objects.equals(sql, that.sql)
        && Objects.equals(schema, that.schema)
        && Objects.equals(fields, that.fields)
        && Objects.equals(dataLocation, that.dataLocation)
        && Objects.equals(workspaceSchemaPath, that.workspaceSchemaPath)
        && Objects.equals(refreshStatus, that.refreshStatus)
        && Objects.equals(lastRefreshTime, that.lastRefreshTime)
        && Objects.equals(lastModifiedTime, that.lastModifiedTime)
        && Objects.equals(additionalMetadata, that.additionalMetadata);
  }

  @Override
  public String toString() {
    return new StringJoiner(",\n", MaterializedViewMetadataUnit.class.getSimpleName() + "[", "]")
        .add("storagePlugin=" + storagePlugin)
        .add("workspace=" + workspace)
        .add("name=" + name)
        .add("owner=" + owner)
        .add("sql=" + sql)
        .add("schema=" + schema)
        .add("fields=" + fields)
        .add("dataLocation=" + dataLocation)
        .add("workspaceSchemaPath=" + workspaceSchemaPath)
        .add("refreshStatus=" + refreshStatus)
        .add("lastRefreshTime=" + lastRefreshTime)
        .add("lastModifiedTime=" + lastModifiedTime)
        .add("additionalMetadata=" + additionalMetadata)
        .toString();
  }

  public static class Builder {
    private String storagePlugin;
    private String workspace;
    private String name;
    private String owner;
    private String sql;
    private String schema;
    private String fields;
    private String dataLocation;
    private List<String> workspaceSchemaPath;
    private String refreshStatus;
    private Long lastRefreshTime;
    private Long lastModifiedTime;
    private String additionalMetadata;

    public Builder storagePlugin(String storagePlugin) {
      this.storagePlugin = storagePlugin;
      return this;
    }

    public Builder workspace(String workspace) {
      this.workspace = workspace;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder owner(String owner) {
      this.owner = owner;
      return this;
    }

    public Builder sql(String sql) {
      this.sql = sql;
      return this;
    }

    public Builder schema(String schema) {
      this.schema = schema;
      return this;
    }

    public Builder fields(String fields) {
      this.fields = fields;
      return this;
    }

    public Builder dataLocation(String dataLocation) {
      this.dataLocation = dataLocation;
      return this;
    }

    public Builder workspaceSchemaPath(List<String> workspaceSchemaPath) {
      this.workspaceSchemaPath = workspaceSchemaPath;
      return this;
    }

    public Builder refreshStatus(String refreshStatus) {
      this.refreshStatus = refreshStatus;
      return this;
    }

    public Builder lastRefreshTime(Long lastRefreshTime) {
      this.lastRefreshTime = lastRefreshTime;
      return this;
    }

    public Builder lastModifiedTime(Long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
      return this;
    }

    public Builder additionalMetadata(String additionalMetadata) {
      this.additionalMetadata = additionalMetadata;
      return this;
    }

    public MaterializedViewMetadataUnit build() {
      return new MaterializedViewMetadataUnit(this);
    }
  }

  /**
   * Contains schema metadata for MaterializedViewMetadataUnit.
   * Provides method handlers for reflection-based field access.
   */
  public static class Schema {

    private final List<MetastoreColumn> columns;
    private final Map<String, MethodHandle> unitGetters;
    private final Map<String, MethodHandle> unitBuilderSetters;

    private Schema(List<MetastoreColumn> columns,
                   Map<String, MethodHandle> unitGetters,
                   Map<String, MethodHandle> unitBuilderSetters) {
      this.columns = columns;
      this.unitGetters = unitGetters;
      this.unitBuilderSetters = unitBuilderSetters;
    }

    public static Schema of(Class<?> unitClass, Class<?> builderClass) {
      List<MetastoreColumn> columns = new ArrayList<>();
      Map<String, MethodHandle> unitGetters = new HashMap<>();
      Map<String, MethodHandle> unitBuilderSetters = new HashMap<>();

      MethodHandles.Lookup gettersLookup = MethodHandles.publicLookup().in(unitClass);
      MethodHandles.Lookup settersLookup = MethodHandles.publicLookup().in(builderClass);

      for (Field field : unitClass.getDeclaredFields()) {
        MetastoreFieldDefinition definition = field.getAnnotation(MetastoreFieldDefinition.class);
        if (definition == null) {
          continue;
        }

        MetastoreColumn column = definition.column();
        columns.add(column);

        Class<?> type = field.getType();
        try {
          String fieldName = field.getName();
          String columnName = column.columnName();
          MethodHandle getter = gettersLookup.findVirtual(unitClass, fieldName, MethodType.methodType(type));
          unitGetters.put(columnName, getter);
          MethodHandle setter = settersLookup.findVirtual(builderClass, fieldName, MethodType.methodType(builderClass, type));
          unitBuilderSetters.put(columnName, setter);
        } catch (ReflectiveOperationException e) {
          throw new MetastoreException(String.format("Unable to init unit setter / getter method handlers " +
              "for unit [%s] and its builder [%s] classes", unitClass.getSimpleName(), builderClass.getSimpleName()), e);
        }
      }

      return new Schema(columns, unitGetters, unitBuilderSetters);
    }

    public List<MetastoreColumn> columns() {
      return columns;
    }

    public Map<String, MethodHandle> unitGetters() {
      return unitGetters;
    }

    public Map<String, MethodHandle> unitBuilderSetters() {
      return unitBuilderSetters;
    }
  }
}
