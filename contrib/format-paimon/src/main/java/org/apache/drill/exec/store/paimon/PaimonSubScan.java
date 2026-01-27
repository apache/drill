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
package org.apache.drill.exec.store.paimon;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
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
import org.apache.drill.exec.store.paimon.format.PaimonFormatPlugin;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@JsonTypeName("paimon-read")
@SuppressWarnings("unused")
public class PaimonSubScan extends AbstractBase implements SubScan {

  private static final String OPERATOR_TYPE = "PAIMON_SUB_SCAN";

  private final PaimonFormatPlugin formatPlugin;

  private final List<SchemaPath> columns;

  private final LogicalExpression condition;

  private final TupleMetadata schema;

  private final List<PaimonWork> workList;

  private final String path;

  private final int maxRecords;

  @JsonCreator
  public PaimonSubScan(
    @JsonProperty("userName") String userName,
    @JsonProperty("storage") StoragePluginConfig storageConfig,
    @JsonProperty("format") FormatPluginConfig formatConfig,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("path") String path,
    @JsonProperty("workList") List<PaimonWork> workList,
    @JsonProperty("schema") TupleMetadata schema,
    @JsonProperty("condition") LogicalExpression condition,
    @JsonProperty("maxRecords") Integer maxRecords,
    @JacksonInject StoragePluginRegistry pluginRegistry) {
    this.formatPlugin = pluginRegistry.resolveFormat(storageConfig, formatConfig, PaimonFormatPlugin.class);
    this.columns = columns;
    this.workList = workList;
    this.path = path;
    this.condition = condition;
    this.schema = schema;
    this.maxRecords = maxRecords;
  }

  private PaimonSubScan(PaimonSubScanBuilder builder) {
    super(builder.userName);
    this.formatPlugin = builder.formatPlugin;
    this.columns = builder.columns;
    this.condition = builder.condition;
    this.schema = builder.schema;
    this.workList = builder.workList;
    this.path = builder.path;
    this.maxRecords = builder.maxRecords;
  }

  public static PaimonSubScanBuilder builder() {
    return new PaimonSubScanBuilder();
  }

  @Override
  public <T, X, E extends Throwable> T accept(
    PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  public List<PaimonWork> getWorkList() {
    return workList;
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

  @JsonIgnore
  public PaimonFormatPlugin getFormatPlugin() {
    return formatPlugin;
  }

  public PaimonSubScanBuilder toBuilder() {
    return new PaimonSubScanBuilder()
      .userName(this.userName)
      .formatPlugin(this.formatPlugin)
      .columns(this.columns)
      .condition(this.condition)
      .schema(this.schema)
      .workList(this.workList)
      .path(this.path)
      .maxRecords(this.maxRecords);
  }

  public static class PaimonSubScanBuilder {
    private String userName;

    private PaimonFormatPlugin formatPlugin;

    private List<SchemaPath> columns;

    private LogicalExpression condition;

    private TupleMetadata schema;

    private List<PaimonWork> workList;

    private String path;

    private int maxRecords;

    public PaimonSubScanBuilder userName(String userName) {
      this.userName = userName;
      return this;
    }

    public PaimonSubScanBuilder formatPlugin(PaimonFormatPlugin formatPlugin) {
      this.formatPlugin = formatPlugin;
      return this;
    }

    public PaimonSubScanBuilder columns(List<SchemaPath> columns) {
      this.columns = columns;
      return this;
    }

    public PaimonSubScanBuilder condition(LogicalExpression condition) {
      this.condition = condition;
      return this;
    }

    public PaimonSubScanBuilder schema(TupleMetadata schema) {
      this.schema = schema;
      return this;
    }

    public PaimonSubScanBuilder workList(List<PaimonWork> workList) {
      this.workList = workList;
      return this;
    }

    public PaimonSubScanBuilder path(String path) {
      this.path = path;
      return this;
    }

    public PaimonSubScanBuilder maxRecords(int maxRecords) {
      this.maxRecords = maxRecords;
      return this;
    }

    public PaimonSubScan build() {
      return new PaimonSubScan(this);
    }
  }
}
