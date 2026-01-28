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
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ExpressionStringBuilder;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.paimon.format.PaimonFormatPlugin;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@JsonTypeName("paimon-scan")
@SuppressWarnings("unused")
public class PaimonGroupScan extends AbstractGroupScan {

  private static final Logger logger = LoggerFactory.getLogger(PaimonGroupScan.class);

  private final PaimonFormatPlugin formatPlugin;

  private final String path;

  private final TupleMetadata schema;

  private final LogicalExpression condition;

  private final List<SchemaPath> columns;

  private int maxRecords;

  private List<PaimonCompleteWork> chunks;

  private List<EndpointAffinity> endpointAffinities;

  private ListMultimap<Integer, PaimonCompleteWork> mappings;

  @JsonCreator
  public PaimonGroupScan(
      @JsonProperty("userName") String userName,
      @JsonProperty("storage") StoragePluginConfig storageConfig,
      @JsonProperty("format") FormatPluginConfig formatConfig,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("schema") TupleMetadata schema,
      @JsonProperty("path") String path,
      @JsonProperty("condition") LogicalExpression condition,
      @JsonProperty("maxRecords") Integer maxRecords,
      @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException {
    this(builder()
      .userName(userName)
      .formatPlugin(pluginRegistry.resolveFormat(storageConfig, formatConfig, PaimonFormatPlugin.class))
      .schema(schema)
      .path(path)
      .condition(condition)
      .columns(columns)
      .maxRecords(maxRecords));
  }

  private PaimonGroupScan(PaimonGroupScanBuilder builder) throws IOException {
    super(builder.userName);
    this.formatPlugin = builder.formatPlugin;
    this.columns = builder.columns;
    this.path = builder.path;
    this.schema = builder.schema;
    this.condition = builder.condition;
    this.maxRecords = builder.maxRecords;

    init();
  }

  private PaimonGroupScan(PaimonGroupScan that) {
    super(that);
    this.columns = that.columns;
    this.formatPlugin = that.formatPlugin;
    this.path = that.path;
    this.condition = that.condition;
    this.schema = that.schema;
    this.maxRecords = that.maxRecords;
    this.chunks = that.chunks;
    this.endpointAffinities = that.endpointAffinities;
    this.mappings = that.mappings;
  }

  public static PaimonGroupScanBuilder builder() {
    return new PaimonGroupScanBuilder();
  }

  @Override
  public PaimonGroupScan clone(List<SchemaPath> columns) {
    try {
      return toBuilder().columns(columns).build();
    } catch (IOException e) {
      throw UserException.dataReadError(e)
        .message("Failed to clone Paimon group scan")
        .build(logger);
    }
  }

  @Override
  public PaimonGroupScan applyLimit(int maxRecords) {
    PaimonGroupScan clone = new PaimonGroupScan(this);
    clone.maxRecords = maxRecords;
    return clone;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    mappings = AssignmentCreator.getMappings(endpoints, chunks);
  }

  private void createMappings(List<EndpointAffinity> affinities) {
    List<DrillbitEndpoint> endpoints = affinities.stream()
      .map(EndpointAffinity::getEndpoint)
      .collect(Collectors.toList());
    applyAssignments(endpoints);
  }

  @Override
  public PaimonSubScan getSpecificScan(int minorFragmentId) {
    if (chunks.isEmpty()) {
      return emptySubScan();
    }

    if (mappings == null) {
      createMappings(endpointAffinities);
    }

    List<PaimonCompleteWork> workList = mappings.get(minorFragmentId);
    List<PaimonWork> paimonWorkList = workList == null
      ? Collections.emptyList()
      : convertWorkList(workList);

    PaimonSubScan subScan = PaimonSubScan.builder()
      .userName(userName)
      .formatPlugin(formatPlugin)
      .columns(columns)
      .condition(condition)
      .schema(schema)
      .workList(paimonWorkList)
      .path(path)
      .maxRecords(maxRecords)
      .build();

    subScan.setOperatorId(getOperatorId());
    return subScan;
  }

  private PaimonSubScan emptySubScan() {
    PaimonSubScan subScan = PaimonSubScan.builder()
      .userName(userName)
      .formatPlugin(formatPlugin)
      .columns(columns)
      .condition(condition)
      .schema(schema)
      .workList(Collections.emptyList())
      .path(path)
      .maxRecords(maxRecords)
      .build();
    subScan.setOperatorId(getOperatorId());
    return subScan;
  }

  private List<PaimonWork> convertWorkList(List<PaimonCompleteWork> workList) {
    return workList.stream()
      .map(PaimonCompleteWork::getSplit)
      .map(PaimonWork::new)
      .collect(Collectors.toList());
  }

  @JsonProperty("maxRecords")
  public int getMaxRecords() {
    return maxRecords;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return Math.max(chunks.size(), 1);
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public ScanStats getScanStats() {
    long rowCount = chunks.stream()
      .mapToLong(PaimonCompleteWork::getRowCount)
      .sum();
    long estimatedRecords = rowCount > 0
      ? rowCount
      : Math.max(chunks.size(), 1) * 1_000_000L;
    return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, estimatedRecords, 1, 0);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new PaimonGroupScan(this);
  }

  private void init() throws IOException {
    Table table = PaimonTableUtils.loadTable(formatPlugin, path);
    String fileFormat = new CoreOptions(table.options()).fileFormatString();
    // Paimon supports multiple formats; Drill currently reads Parquet/ORC only.
    if (!"parquet".equalsIgnoreCase(fileFormat) && !"orc".equalsIgnoreCase(fileFormat)) {
      throw UserException.unsupportedError()
        .message("Paimon file format '%s' is not supported. Only parquet and orc are supported.", fileFormat)
        .build(logger);
    }

    RowType rowType = table.rowType();
    ReadBuilder readBuilder = table.newReadBuilder();
    PaimonReadUtils.applyFilter(readBuilder, rowType, condition);
    PaimonReadUtils.applyProjection(readBuilder, rowType, columns);
    TableScan tableScan = readBuilder.newScan();
    List<Split> splits = tableScan.plan().splits();
    chunks = splits.stream()
      .map(split -> new PaimonCompleteWork(new EndpointByteMapImpl(), split))
      .collect(Collectors.toList());
    endpointAffinities = AffinityCreator.getAffinityMap(chunks);
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    if (endpointAffinities == null) {
      logger.debug("Chunks size: {}", chunks.size());
      endpointAffinities = AffinityCreator.getAffinityMap(chunks);
    }
    return endpointAffinities;
  }

  @Override
  public boolean supportsLimitPushdown() {
    return true;
  }

  @Override
  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty("schema")
  public TupleMetadata getSchema() {
    return schema;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getStorageConfig() {
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty("format")
  public FormatPluginConfig getFormatConfig() {
    return formatPlugin.getConfig();
  }

  @JsonProperty("path")
  public String getPath() {
    return path;
  }

  @JsonProperty("condition")
  public LogicalExpression getCondition() {
    return condition;
  }

  @JsonIgnore
  public PaimonFormatPlugin getFormatPlugin() {
    return formatPlugin;
  }

  @Override
  public String getOperatorType() {
    return "PAIMON_GROUP_SCAN";
  }

  @Override
  public String toString() {
    String conditionString = condition == null ? null : ExpressionStringBuilder.toString(condition).trim();
    return new PlanStringBuilder(this)
      .field("path", path)
      .field("schema", schema)
      .field("columns", columns)
      .field("condition", conditionString)
      .field("maxRecords", maxRecords)
      .toString();
  }

  public PaimonGroupScanBuilder toBuilder() {
    return new PaimonGroupScanBuilder()
      .userName(this.userName)
      .formatPlugin(this.formatPlugin)
      .schema(this.schema)
      .path(this.path)
      .condition(this.condition)
      .columns(this.columns)
      .maxRecords(this.maxRecords);
  }

  public static class PaimonGroupScanBuilder {
    private String userName;

    private PaimonFormatPlugin formatPlugin;

    private TupleMetadata schema;

    private String path;

    private LogicalExpression condition;

    private List<SchemaPath> columns;

    private int maxRecords;

    public PaimonGroupScanBuilder userName(String userName) {
      this.userName = userName;
      return this;
    }

    public PaimonGroupScanBuilder formatPlugin(PaimonFormatPlugin formatPlugin) {
      this.formatPlugin = formatPlugin;
      return this;
    }

    public PaimonGroupScanBuilder schema(TupleMetadata schema) {
      this.schema = schema;
      return this;
    }

    public PaimonGroupScanBuilder path(String path) {
      this.path = path;
      return this;
    }

    public PaimonGroupScanBuilder condition(LogicalExpression condition) {
      this.condition = condition;
      return this;
    }

    public PaimonGroupScanBuilder columns(List<SchemaPath> columns) {
      this.columns = columns;
      return this;
    }

    public PaimonGroupScanBuilder maxRecords(int maxRecords) {
      this.maxRecords = maxRecords;
      return this;
    }

    public PaimonGroupScan build() throws IOException {
      return new PaimonGroupScan(this);
    }
  }
}
