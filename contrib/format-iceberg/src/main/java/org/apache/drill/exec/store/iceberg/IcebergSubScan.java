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
package org.apache.drill.exec.store.iceberg;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Builder;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.iceberg.format.IcebergFormatPlugin;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.iceberg.TableScan;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@JsonTypeName("iceberg-read")
@SuppressWarnings("unused")
public class IcebergSubScan extends AbstractBase implements SubScan {

  private static final String OPERATOR_TYPE = "ICEBERG_SUB_SCAN";

  private final IcebergFormatPlugin formatPlugin;

  private final List<SchemaPath> columns;

  private final LogicalExpression condition;

  private final TupleMetadata schema;

  private final List<IcebergWork> workList;

  private final TableScan tableScan;

  private final String path;

  private final int maxRecords;

  @JsonCreator
  public IcebergSubScan(
    @JsonProperty("userName") String userName,
    @JsonProperty("storage") StoragePluginConfig storageConfig,
    @JsonProperty("format") FormatPluginConfig formatConfig,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("path") String path,
    @JsonProperty("workList") List<IcebergWork> workList,
    @JsonProperty("schema") TupleMetadata schema,
    @JsonProperty("condition") LogicalExpression condition,
    @JsonProperty("maxRecords") Integer maxRecords,
    @JacksonInject StoragePluginRegistry pluginRegistry) {
    this.formatPlugin = pluginRegistry.resolveFormat(storageConfig, formatConfig, IcebergFormatPlugin.class);
    this.columns = columns;
    this.workList = workList;
    this.path = path;
    this.condition = condition;
    this.tableScan = getTableScan(columns, path, condition);
    this.schema = schema;
    this.maxRecords = maxRecords;
  }

  @Builder(toBuilder = true)
  private IcebergSubScan(String userName, IcebergFormatPlugin formatPlugin,
    List<SchemaPath> columns, LogicalExpression condition, TupleMetadata schema,
    List<IcebergWork> workList, TableScan tableScan, String path, int maxRecords) {
    super(userName);
    this.formatPlugin = formatPlugin;
    this.columns = columns;
    this.condition = condition;
    this.schema = schema;
    this.workList = workList;
    this.tableScan = tableScan;
    this.path = path;
    this.maxRecords = maxRecords;
  }

  private TableScan getTableScan(List<SchemaPath> columns, String path, LogicalExpression condition) {
    return IcebergGroupScan.projectColumns(
      IcebergGroupScan.initTableScan(formatPlugin, path, condition),
      columns);
  }

  @Override
  public <T, X, E extends Throwable> T accept(
    PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  public List<IcebergWork> getWorkList() {
    return workList;
  }

  @JsonIgnore
  public TableScan getTableScan() {
    return tableScan;
  }

  public int getMaxRecords() {
    return maxRecords;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  public LogicalExpression getCondition() {
    return condition;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return this.toBuilder().build();
  }

  @JsonProperty("storage")
  public StoragePluginConfig getStorageConfig() {
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty("format")
  public FormatPluginConfig getFormatConfig() {
    return formatPlugin.getConfig();
  }

  public String getPath() {
    return path;
  }

  @Override
  public String getOperatorType() {
    return OPERATOR_TYPE;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  public TupleMetadata getSchema() {
    return schema;
  }
}
