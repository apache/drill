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
package org.apache.drill.metastore.components.tables;

import org.apache.drill.metastore.MetastoreFieldDefinition;
import org.apache.drill.metastore.exceptions.MetastoreException;
import org.apache.drill.metastore.metadata.MetadataType;

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

import static org.apache.drill.metastore.metadata.MetadataType.ALL;
import static org.apache.drill.metastore.metadata.MetadataType.FILE;
import static org.apache.drill.metastore.metadata.MetadataType.PARTITION;
import static org.apache.drill.metastore.metadata.MetadataType.ROW_GROUP;
import static org.apache.drill.metastore.metadata.MetadataType.SEGMENT;
import static org.apache.drill.metastore.metadata.MetadataType.TABLE;

/**
 * Class that represents one row in Drill Metastore Tables which is a generic representation of metastore metadata
 * suitable to any metastore table metadata type (table, segment, file, row group, partition).
 * Is used to read and write data from / to Drill Metastore Tables implementation.
 *
 * Note: changing field order and adding new fields may break backward compatibility in existing
 * Metastore implementations. It is recommended to add new information into {@link #additionalMetadata}
 * field instead.
 */
public class TableMetadataUnit {

  public static final Schema SCHEMA = Schema.of(TableMetadataUnit.class, Builder.class);

  @MetastoreFieldDefinition(scopes = {ALL}) private final String storagePlugin;
  @MetastoreFieldDefinition(scopes = {ALL}) private final String workspace;
  @MetastoreFieldDefinition(scopes = {ALL}) private final String tableName;
  @MetastoreFieldDefinition(scopes = {TABLE}) private final String owner;
  @MetastoreFieldDefinition(scopes = {TABLE}) private final String tableType;
  @MetastoreFieldDefinition(scopes = {ALL}) private final String metadataType;
  @MetastoreFieldDefinition(scopes = {ALL}) private final String metadataKey;
  @MetastoreFieldDefinition(scopes = {TABLE, SEGMENT, FILE, ROW_GROUP}) private final String location;
  @MetastoreFieldDefinition(scopes = {TABLE}) private final List<String> interestingColumns;
  @MetastoreFieldDefinition(scopes = {ALL}) private final String schema;
  @MetastoreFieldDefinition(scopes = {ALL}) private final Map<String, String> columnsStatistics;
  @MetastoreFieldDefinition(scopes = {ALL}) private final List<String> metadataStatistics;
  @MetastoreFieldDefinition(scopes = {ALL}) private final Long lastModifiedTime;
  @MetastoreFieldDefinition(scopes = {TABLE}) private final Map<String, String> partitionKeys;
  @MetastoreFieldDefinition(scopes = {ALL}) private final String additionalMetadata;

  @MetastoreFieldDefinition(scopes = {SEGMENT, FILE, ROW_GROUP, PARTITION}) private final String metadataIdentifier;
  @MetastoreFieldDefinition(scopes = {SEGMENT, PARTITION}) private final String column;
  @MetastoreFieldDefinition(scopes = {SEGMENT, PARTITION}) private final List<String> locations;
  @MetastoreFieldDefinition(scopes = {SEGMENT, PARTITION}) private final List<String> partitionValues;

  @MetastoreFieldDefinition(scopes = {SEGMENT, FILE, ROW_GROUP}) private final String path;

  @MetastoreFieldDefinition(scopes = {ROW_GROUP}) private final Integer rowGroupIndex;
  @MetastoreFieldDefinition(scopes = {ROW_GROUP}) private final Map<String, Float> hostAffinity;

  private TableMetadataUnit(Builder builder) {
    this.storagePlugin = builder.storagePlugin;
    this.workspace = builder.workspace;
    this.tableName = builder.tableName;
    this.owner = builder.owner;
    this.tableType = builder.tableType;
    this.metadataType = builder.metadataType;
    this.metadataKey = builder.metadataKey;
    this.location = builder.location;
    this.interestingColumns = builder.interestingColumns;
    this.schema = builder.schema;
    this.columnsStatistics = builder.columnsStatistics;
    this.metadataStatistics = builder.metadataStatistics;
    this.lastModifiedTime = builder.lastModifiedTime;
    this.partitionKeys = builder.partitionKeys;
    this.additionalMetadata = builder.additionalMetadata;
    this.metadataIdentifier = builder.metadataIdentifier;
    this.column = builder.column;
    this.locations = builder.locations;
    this.partitionValues = builder.partitionValues;
    this.path = builder.path;
    this.rowGroupIndex = builder.rowGroupIndex;
    this.hostAffinity = builder.hostAffinity;
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

  public String tableName() {
    return tableName;
  }

  public String owner() {
    return owner;
  }

  public String tableType() {
    return tableType;
  }

  public String metadataType() {
    return metadataType;
  }

  public String metadataKey() {
    return metadataKey;
  }

  public String location() {
    return location;
  }

  public List<String> interestingColumns() {
    return interestingColumns;
  }

  public String schema() {
    return schema;
  }

  public Map<String, String> columnsStatistics() {
    return columnsStatistics;
  }

  public List<String> metadataStatistics() {
    return metadataStatistics;
  }

  public Long lastModifiedTime() {
    return lastModifiedTime;
  }

  public Map<String, String> partitionKeys() {
    return partitionKeys;
  }

  public String additionalMetadata() {
    return additionalMetadata;
  }

  public String metadataIdentifier() {
    return metadataIdentifier;
  }

  public String column() {
    return column;
  }

  public List<String> locations() {
    return locations;
  }

  public List<String> partitionValues() {
    return partitionValues;
  }

  public String path() {
    return path;
  }

  public Integer rowGroupIndex() {
    return rowGroupIndex;
  }

  public Map<String, Float> hostAffinity() {
    return hostAffinity;
  }

  public Builder toBuilder() {
    return TableMetadataUnit.builder()
      .storagePlugin(storagePlugin)
      .workspace(workspace)
      .tableName(tableName)
      .owner(owner)
      .tableType(tableType)
      .metadataType(metadataType)
      .metadataKey(metadataKey)
      .location(location)
      .interestingColumns(interestingColumns)
      .schema(schema)
      .columnsStatistics(columnsStatistics)
      .metadataStatistics(metadataStatistics)
      .lastModifiedTime(lastModifiedTime)
      .partitionKeys(partitionKeys)
      .additionalMetadata(additionalMetadata)
      .metadataIdentifier(metadataIdentifier)
      .column(column)
      .locations(locations)
      .partitionValues(partitionValues)
      .path(path)
      .rowGroupIndex(rowGroupIndex)
      .hostAffinity(hostAffinity);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storagePlugin, workspace, tableName, owner, tableType, metadataType,
      metadataKey, location, interestingColumns, schema, columnsStatistics, metadataStatistics,
      lastModifiedTime, partitionKeys, additionalMetadata, metadataIdentifier, column, locations,
      partitionValues, path, rowGroupIndex, hostAffinity);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)  {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableMetadataUnit that = (TableMetadataUnit) o;
    return Objects.equals(storagePlugin, that.storagePlugin)
      && Objects.equals(workspace, that.workspace)
      && Objects.equals(tableName, that.tableName)
      && Objects.equals(owner, that.owner)
      && Objects.equals(tableType, that.tableType)
      && Objects.equals(metadataType, that.metadataType)
      && Objects.equals(metadataKey, that.metadataKey)
      && Objects.equals(location, that.location)
      && Objects.equals(interestingColumns, that.interestingColumns)
      && Objects.equals(schema, that.schema)
      && Objects.equals(columnsStatistics, that.columnsStatistics)
      && Objects.equals(metadataStatistics, that.metadataStatistics)
      && Objects.equals(lastModifiedTime, that.lastModifiedTime)
      && Objects.equals(partitionKeys, that.partitionKeys)
      && Objects.equals(additionalMetadata, that.additionalMetadata)
      && Objects.equals(metadataIdentifier, that.metadataIdentifier)
      && Objects.equals(column, that.column)
      && Objects.equals(locations, that.locations)
      && Objects.equals(partitionValues, that.partitionValues)
      && Objects.equals(path, that.path)
      && Objects.equals(rowGroupIndex, that.rowGroupIndex)
      && Objects.equals(hostAffinity, that.hostAffinity);
  }

  @Override
  public String toString() {
    return new StringJoiner(",\n", TableMetadataUnit.class.getSimpleName() + "[", "]")
      .add("storagePlugin=" + storagePlugin)
      .add("workspace=" + workspace)
      .add("tableName=" + tableName)
      .add("owner=" + owner)
      .add("tableType=" + tableType)
      .add("metadataType=" + metadataType)
      .add("metadataKey=" + metadataKey)
      .add("location=" + location)
      .add("interestingColumns=" + interestingColumns)
      .add("schema=" + schema)
      .add("columnsStatistics=" + columnsStatistics)
      .add("metadataStatistics=" + metadataStatistics)
      .add("lastModifiedTime=" + lastModifiedTime)
      .add("partitionKeys=" + partitionKeys)
      .add("additionalMetadata=" + additionalMetadata)
      .add("metadataIdentifier=" + metadataIdentifier)
      .add("column=" + column)
      .add("locations=" + locations)
      .add("partitionValues=" + partitionValues)
      .add("path=" + path)
      .add("rowGroupIndex=" + rowGroupIndex)
      .add("hostAffinity=" + hostAffinity)
      .toString();
  }

  public static class Builder {

    private String storagePlugin;
    private String workspace;
    private String tableName;
    private String owner;
    private String tableType;
    private String metadataType;
    private String metadataKey;
    private String location;
    private List<String> interestingColumns;
    private String schema;
    private Map<String, String> columnsStatistics;
    private List<String> metadataStatistics;
    private Long lastModifiedTime;
    private Map<String, String> partitionKeys;
    private String additionalMetadata;
    private String metadataIdentifier;
    private String column;
    private List<String> locations;
    private List<String> partitionValues;
    private String path;
    private Integer rowGroupIndex;
    private Map<String, Float> hostAffinity;

    public Builder storagePlugin(String storagePlugin) {
      this.storagePlugin = storagePlugin;
      return this;
    }

    public Builder workspace(String workspace) {
      this.workspace = workspace;
      return this;
    }

    public Builder tableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder owner(String owner) {
      this.owner = owner;
      return this;
    }

    public Builder tableType(String tableType) {
      this.tableType = tableType;
      return this;
    }

    public Builder metadataType(String metadataType) {
      this.metadataType = metadataType;
      return this;
    }

    public Builder metadataKey(String metadataKey) {
      this.metadataKey = metadataKey;
      return this;
    }

    public Builder location(String location) {
      this.location = location;
      return this;
    }

    public Builder interestingColumns(List<String> interestingColumns) {
      this.interestingColumns = interestingColumns;
      return this;
    }

    public Builder schema(String schema) {
      this.schema = schema;
      return this;
    }

    public Builder columnsStatistics(Map<String, String> columnsStatistics) {
      this.columnsStatistics = columnsStatistics;
      return this;
    }

    public Builder metadataStatistics(List<String> metadataStatistics) {
      this.metadataStatistics = metadataStatistics;
      return this;
    }

    public Builder lastModifiedTime(Long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
      return this;
    }

    public Builder partitionKeys(Map<String, String> partitionKeys) {
      this.partitionKeys = partitionKeys;
      return this;
    }

    public Builder additionalMetadata(String additionalMetadata) {
      this.additionalMetadata = additionalMetadata;
      return this;
    }

    public Builder metadataIdentifier(String metadataIdentifier) {
      this.metadataIdentifier = metadataIdentifier;
      return this;
    }

    public Builder column(String column) {
      this.column = column;
      return this;
    }

    public Builder locations(List<String> locations) {
      this.locations = locations;
      return this;
    }

    public Builder partitionValues(List<String> partitionValues) {
      this.partitionValues = partitionValues;
      return this;
    }

    public Builder path(String path) {
      this.path = path;
      return this;
    }

    public Builder rowGroupIndex(Integer rowGroupIndex) {
      this.rowGroupIndex = rowGroupIndex;
      return this;
    }

    public Builder hostAffinity(Map<String, Float> hostAffinity) {
      this.hostAffinity = hostAffinity;
      return this;
    }

    public TableMetadataUnit build() {
      return new TableMetadataUnit(this);
    }
  }

  /**
   * Contains schema metadata, including lists of columns which belong to table, segment, file, row group
   * or partition. Also provides unit class getters and its builder setters method handlers
   * to optimize reflection calls.
   */
  public static class Schema {

    private final List<String> tableColumns;
    private final List<String> segmentColumns;
    private final List<String> fileColumns;
    private final List<String> rowGroupColumns;
    private final List<String> partitionColumns;
    private final Map<String, MethodHandle> unitGetters;
    private final Map<String, MethodHandle> unitBuilderSetters;

    private Schema(List<String> tableColumns,
                   List<String> segmentColumns,
                   List<String> fileColumns,
                   List<String> rowGroupColumns,
                   List<String> partitionColumns,
                   Map<String, MethodHandle> unitGetters,
                   Map<String, MethodHandle> unitBuilderSetters) {
      this.tableColumns = tableColumns;
      this.segmentColumns = segmentColumns;
      this.fileColumns = fileColumns;
      this.rowGroupColumns = rowGroupColumns;
      this.partitionColumns = partitionColumns;
      this.unitGetters = unitGetters;
      this.unitBuilderSetters = unitBuilderSetters;
    }

    /**
     * Obtains field information for the given unit class and its builder.
     * Traverses through the list of unit class fields which are annotated with {@link MetastoreFieldDefinition}
     * and creates instance of {@link Schema} class that holds unit class schema metadata.
     * Assumes that given unit class and its builder getters and setters method names
     * are the same as annotated fields names.
     */
    public static Schema of(Class<?> unitClass, Class<?> builderClass) {
      List<String> tableColumns = new ArrayList<>();
      List<String> segmentColumns = new ArrayList<>();
      List<String> fileColumns = new ArrayList<>();
      List<String> rowGroupColumns = new ArrayList<>();
      List<String> partitionColumns = new ArrayList<>();
      Map<String, MethodHandle> unitGetters = new HashMap<>();
      Map<String, MethodHandle> unitBuilderSetters = new HashMap<>();

      MethodHandles.Lookup gettersLookup = MethodHandles.publicLookup().in(unitClass);
      MethodHandles.Lookup settersLookup = MethodHandles.publicLookup().in(builderClass);

      for (Field field : unitClass.getDeclaredFields()) {
        MetastoreFieldDefinition definition = field.getAnnotation(MetastoreFieldDefinition.class);
        if (definition == null) {
          continue;
        }

        String name = field.getName();
        for (MetadataType scope : definition.scopes()) {
          switch (scope) {
            case TABLE:
              tableColumns.add(name);
              break;
            case SEGMENT:
              segmentColumns.add(name);
              break;
            case FILE:
              fileColumns.add(name);
              break;
            case ROW_GROUP:
              rowGroupColumns.add(name);
              break;
            case PARTITION:
              partitionColumns.add(name);
              break;
            case ALL:
              tableColumns.add(name);
              segmentColumns.add(name);
              fileColumns.add(name);
              rowGroupColumns.add(name);
              partitionColumns.add(name);
              break;
            default:
              throw new IllegalStateException(scope.name());
          }
        }

        Class<?> type = field.getType();
        try {
          MethodHandle getter = gettersLookup.findVirtual(unitClass, name, MethodType.methodType(type));
          unitGetters.put(name, getter);
          MethodHandle setter = settersLookup.findVirtual(builderClass, name, MethodType.methodType(builderClass, type));
          unitBuilderSetters.put(name, setter);
        } catch (ReflectiveOperationException e) {
          throw new MetastoreException(String.format("Unable to init unit setter / getter method handlers " +
              "for unit [%s] and its builder [%s] classes", unitClass.getSimpleName(), builderClass.getSimpleName()), e);
        }
      }

      return new Schema(tableColumns, segmentColumns, fileColumns, rowGroupColumns, partitionColumns,
        unitGetters, unitBuilderSetters);
    }

    public List<String> tableColumns() {
      return tableColumns;
    }

    public List<String> segmentColumns() {
      return segmentColumns;
    }

    public List<String> fileColumns() {
      return fileColumns;
    }

    public List<String> rowGroupColumns() {
      return rowGroupColumns;
    }

    public List<String> partitionColumns() {
      return partitionColumns;
    }

    public Map<String, MethodHandle> unitGetters() {
      return unitGetters;
    }

    public Map<String, MethodHandle> unitBuilderSetters() {
      return unitBuilderSetters;
    }
  }
}
