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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.base.filter.ExprNode;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.metastore.metadata.TableMetadata;
import org.apache.drill.metastore.metadata.TableMetadataProvider;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonTypeName("googlesheets-group-scan")
public class GoogleSheetsGroupScan extends AbstractGroupScan {

  private final GoogleSheetsScanSpec scanSpec;
  private final GoogleSheetsStoragePluginConfig config;
  private final List<SchemaPath> columns;
  private final String pluginName;
  private final Map<String, ExprNode.ColRelOpConstNode> filters;
  private final ScanStats scanStats;
  private final double filterSelectivity;
  private final int maxRecords;
  private final GoogleSheetsStoragePlugin plugin;
  private int hashCode;
  private MetadataProviderManager metadataProviderManager;

  // Initial Constructor
  public GoogleSheetsGroupScan(String userName,
                               GoogleSheetsScanSpec scanSpec,
                               GoogleSheetsStoragePlugin plugin,
                               MetadataProviderManager metadataProviderManager) {
    super(userName);
    this.scanSpec = scanSpec;
    this.config = scanSpec.getConfig();
    this.columns = ALL_COLUMNS;
    this.pluginName = plugin.getName();
    this.filters = null;
    this.filterSelectivity = 0.0;
    this.maxRecords = -1;
    this.scanStats = computeScanStats();
    this.plugin = plugin;
    this.metadataProviderManager = metadataProviderManager;
  }

  // Copy Constructor
  public GoogleSheetsGroupScan(GoogleSheetsGroupScan that) {
    super(that);
    this.scanSpec = that.scanSpec;
    this.config = that.config;
    this.columns = that.columns;
    this.filters = that.filters;
    this.pluginName = that.pluginName;
    this.filterSelectivity = that.filterSelectivity;
    this.scanStats = that.scanStats;
    this.maxRecords = that.maxRecords;
    this.plugin = that.plugin;
    this.metadataProviderManager = that.metadataProviderManager;
    this.hashCode = hashCode();
  }

  /**
   * Constructor for applying a limit.
   * @param that The previous group scan without the limit.
   * @param maxRecords  The desired limit, pushed down from Calcite
   */
  public GoogleSheetsGroupScan(GoogleSheetsGroupScan that, int maxRecords) {
    super(that);
    this.scanSpec = that.scanSpec;
    this.config = that.config;
    this.columns = that.columns;
    this.pluginName = that.pluginName;
    this.filters = that.filters;
    this.filterSelectivity = that.filterSelectivity;
    this.maxRecords = maxRecords;
    this.plugin = that.plugin;
    this.metadataProviderManager = that.metadataProviderManager;
    this.scanStats = computeScanStats();
  }

  /**
   * Constructor for applying columns (Projection pushdown).
   * @param that The previous GroupScan, without the columns
   * @param columns The list of columns to push down
   */
  public GoogleSheetsGroupScan(GoogleSheetsGroupScan that, List<SchemaPath> columns) {
    super(that);
    this.scanSpec = that.scanSpec;
    this.config = scanSpec.getConfig();
    this.columns = columns;
    this.filters = that.filters;
    this.pluginName = that.pluginName;
    this.filterSelectivity = that.filterSelectivity;
    this.maxRecords = that.maxRecords;
    this.plugin = that.plugin;
    this.metadataProviderManager = that.metadataProviderManager;
    this.scanStats = computeScanStats();
  }

  /**
   * Constructor for applying a filter
   * @param that Previous group scan w/o filters
   * @param filters The list of filters
   * @param filterSelectivity  The filter selectivity
   */
  public GoogleSheetsGroupScan(GoogleSheetsGroupScan that,
                               Map<String, ExprNode.ColRelOpConstNode> filters,
                               double filterSelectivity) {
    super(that);
    this.scanSpec = that.scanSpec;
    this.config = that.config;
    this.columns = that.columns;
    this.filters = filters;
    this.pluginName = that.pluginName;
    this.filterSelectivity = filterSelectivity;
    this.maxRecords = that.maxRecords;
    this.plugin = that.plugin;
    this.metadataProviderManager = that.metadataProviderManager;
    this.scanStats = computeScanStats();
  }

  @JsonCreator
  public GoogleSheetsGroupScan(
    @JsonProperty("userName") String userName,
    @JsonProperty("scanSpec") GoogleSheetsScanSpec scanSpec,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("filters") Map<String, ExprNode.ColRelOpConstNode> filters,
    @JsonProperty("filterSelectivity") double selectivity,
    @JsonProperty("maxRecords") int maxRecords,
    @JacksonInject StoragePluginRegistry plugins
  ) {
    super(userName);
    this.scanSpec = scanSpec;
    this.config = scanSpec.getConfig();
    this.columns = columns;
    this.filters = filters;
    this.filterSelectivity = selectivity;
    this.maxRecords = maxRecords;
    this.scanStats = computeScanStats();
    this.plugin = plugins.resolve(config, GoogleSheetsStoragePlugin.class);
    this.pluginName = plugin.getName();
  }

  @JsonProperty("scanSpec")
  public GoogleSheetsScanSpec scanSpec() {
    return scanSpec;
  }

  @JsonProperty("config")
  public GoogleSheetsStoragePluginConfig config() {
    return config;
  }

  @JsonProperty("columns")
  public List<SchemaPath> columns() {
    return columns;
  }

  @JsonProperty("filters")
  public Map<String, ExprNode.ColRelOpConstNode> filters() {
    return filters;
  }

  @JsonProperty("maxRecords")
  public int maxRecords() {
    return maxRecords;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {

  }

  public TupleMetadata getSchema() {
    if (metadataProviderManager == null) {
      return null;
    }
    try {
      return metadataProviderManager.getSchemaProvider().read().getSchema();
    } catch (IOException | NullPointerException e) {
      return null;
    }
  }

  @Override
  public TableMetadata getTableMetadata() {
    if (getMetadataProvider() == null) {
      return null;
    }
    return getMetadataProvider().getTableMetadata();
  }

  @Override
  public TableMetadataProvider getMetadataProvider() {
    if (metadataProviderManager == null) {
      return null;
    }
    return metadataProviderManager.getTableMetadataProvider();
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @JsonIgnore
  public boolean allowsFilters() {
    return true;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    return new GoogleSheetsSubScan(userName, config, scanSpec, columns, filters, maxRecords, getSchema());
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return new GoogleSheetsGroupScan(this, columns);
  }

  @Override
  public boolean supportsLimitPushdown() {
    return true;
  }

  @Override
  public GroupScan applyLimit(int maxRecords) {
    if (maxRecords == this.maxRecords) {
      return null;
    }
    return new GoogleSheetsGroupScan(this, maxRecords);
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public ScanStats getScanStats() {

    // Since this class is immutable, compute stats once and cache
    // them. If the scan changes (adding columns, adding filters), we
    // get a new scan without cached stats.
    return scanStats;
  }

  private ScanStats computeScanStats() {

    // If this config allows filters, then make the default
    // cost very high to force the planner to choose the version
    // with filters.
    if (!hasFilters()) {
      return new ScanStats(ScanStats.GroupScanProperty.ESTIMATED_TOTAL_COST,
        1E9, 1E112, 1E12);
    }

    // No good estimates at all, just make up something.
    // TODO It may be possible to obtain this from the Google SDK.
    double estRowCount = 10_000;
    if (maxRecords > 0) {
      estRowCount = maxRecords;
    }

    // NOTE this was important! if the predicates don't make the query more
    // efficient they won't get pushed down
    if (hasFilters()) {
      estRowCount *= filterSelectivity;
    }

    double estColCount = Utilities.isStarQuery(columns) ? DrillScanRel.STAR_COLUMN_COST : columns.size();
    double valueCount = estRowCount * estColCount;
    double cpuCost = valueCount;
    double ioCost = valueCount;

    // Force the caller to use our costs rather than the
    // defaults (which sets IO cost to zero).
    return new ScanStats(ScanStats.GroupScanProperty.ESTIMATED_TOTAL_COST,
      estRowCount, cpuCost, ioCost);
  }

  @JsonIgnore
  public boolean hasFilters() {
    return filters != null;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new GoogleSheetsGroupScan(this);
  }

  @Override
  public int hashCode() {
    if (hashCode == 0) {
      hashCode = Objects.hash(scanSpec, config, columns, filters, maxRecords, pluginName);
    }
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    GoogleSheetsGroupScan other = (GoogleSheetsGroupScan) obj;
    return Objects.equals(scanSpec, other.scanSpec) &&
      Objects.equals(config, other.config) &&
      Objects.equals(columns, other.columns) &&
      Objects.equals(filters, other.filters) &&
      Objects.equals(maxRecords, other.maxRecords);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("scanSpec", scanSpec)
      .field("filters", filters)
      .field("columns", columns)
      .field("field selectivity", filterSelectivity)
      .field("maxRecords", maxRecords)
      .toString();
  }
}
