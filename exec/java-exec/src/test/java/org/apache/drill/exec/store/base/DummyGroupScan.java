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
package org.apache.drill.exec.store.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.base.filter.FilterSpec;
import org.apache.drill.exec.store.base.filter.RelOp;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Example of a simple group scan. Shows the multiple constructors needed.
 * Tests project and filter-push down.
 */
@JsonTypeName("dummy-scan")
// Force a specific order. Tests compare plans against "golden"
// versions; forcing the order ensures that the plans don't have
// trivial variations if we change this class.
@JsonPropertyOrder({"userName", "scanSpec", "columns",
                    "filters", "cost", "config"})
@JsonInclude(value = Include.NON_EMPTY, content = Include.NON_NULL)
public class DummyGroupScan extends BaseGroupScan {

  private final DummyScanSpec scanSpec;
  private final FilterSpec filters;

  /**
   * Constructor used to first create a "blank" group scan given a
   * scan spec obtained from the {@link DummySchemaFactory}.
   * @param storagePlugin storage plugin that provided the schema factory
   * @param userName the OS user name of the user running the query
   * @param scanSpec the scan spec created by the schema factory
   */
  public DummyGroupScan(DummyStoragePlugin storagePlugin, String userName,
      DummyScanSpec scanSpec) {
    super(storagePlugin, userName, null);
    this.scanSpec = scanSpec;
    filters = null;
  }

  /**
   * Constructor used to add projection push-down columns to the
   * group scan. Copies all fields from an existing group scan,
   * adds a set of projected columns.
   * <p>
   * Note: Calcite is a cost-based optimizer (CBO). Calcite will
   * choose this version of the group scan <b>only</b> if the cost
   * is lower than the previous version. Adjust the cost estimate
   * accordingly.
   *
   * @param from the group scan before projection push-down
   * @param columns the set of columns to project
   */
  public DummyGroupScan(DummyGroupScan from, List<SchemaPath> columns) {
    super(from.storagePlugin, from.getUserName(), columns);
    this.scanSpec = from.scanSpec;
    this.filters = from.filters;
  }

  /**
   * Group scans are serialized as part of the logical plan. This constructor
   * is used to deserialize the group scan from JSON.
   */
  @JsonCreator
  public DummyGroupScan(
      @JsonProperty("config") DummyStoragePluginConfig config,
      @JsonProperty("userName") String userName,
      @JsonProperty("scanSpec") DummyScanSpec scanSpec,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("filters") FilterSpec filters,
      @JacksonInject StoragePluginRegistry engineRegistry) {
    super(config, userName, columns, engineRegistry);
    this.scanSpec = scanSpec;
    this.filters = filters;
  }

  /**
   * Copy constructor to add a filter push-down.
   * <p>
   * As with projection push down, the Calcite CBO will use this
   * new copy <b>only</b> if the cost is lower than the original
   * version. Adjust the cost to reflect filter push-down.
   *
   * @param from the original group scan.
   * @param filters
   */
  public DummyGroupScan(DummyGroupScan from, FilterSpec filters) {
    super(from);
    this.scanSpec = from.scanSpec;
    this.filters = filters;
  }

  @JsonProperty("scanSpec")
  public DummyScanSpec scanSpec() { return scanSpec; }

  @JsonProperty("filters")
  public FilterSpec andFilters() { return filters; }

  public boolean hasFilters() {
    return filters != null && ! filters.isEmpty();
  }

  private static final List<String> FILTER_COLS = Arrays.asList("a", "b", "id");

  public RelOp acceptFilter(RelOp relOp) {

    // Pretend that "id" is a special integer column. Can handle
    // equality only.
    if ("id".equals(relOp.colName)) {

      // To allow easier testing, require exact type match: no
      // attempt at type conversion here.
      if (relOp.op != RelOp.Op.EQ || relOp.value.type != MinorType.INT) {
        return null;
      }
      return relOp;
    }

    // "allTypes" table filters everything. All other tables
    // only project a fixed set of columns. Simulates a plugin
    // which can project only some columns.
    if (!"allTypes".equals(scanSpec.tableName) && !FILTER_COLS.contains(relOp.colName)) {
      return null;
    }

    // Only supports a few operators so we can verify that the
    // others are left in the WHERE clause.
    // Neither are really implemented. Less-than lets us check
    // inverting operations for the const op col case.
    switch (relOp.op) {
    case EQ:
    case LT:
    case LE:

      // Convert to target type (pretend all columns are convertible to VARCHAR)
      return relOp.normalize(relOp.value.toVarChar());
    case IS_NULL:
    case IS_NOT_NULL:
      return relOp;
    default:
      return null;
    }
  }

  @Override
  public ScanStats computeScanStats() {

    // No good estimates at all, just make up something.
    // Simulates a REST call with no metadata; we just want to prevent
    // Drill from thinking it can broadcast the result set. If a real
    // source can estimate the row count, it should do so.
    int estRowCount = 10_000;

    // If filter push down, assume this reduces data size.
    // Need to get Calcite to choose this version rather
    // than the un-filtered version, so mimic the same
    // selectivity that Calcite uses.
    estRowCount = FilterSpec.applySelectivity(filters, estRowCount);

    // Pretend to be an HTTP client, so no disk I/O.
    // Instead we have to explain costs by reducing CPU.
    double cpuRatio = 1.0;

    // If columns provided, then assume this saves data transfer
    if (getColumns() != BaseGroupScan.ALL_COLUMNS) {
      cpuRatio = 0.75;
    }

    // Would like to reduce network costs to simulate an HTTP RPC
    // call, but it is not easy to do here since Drill
    // scans assume we read from disk, not network.
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, estRowCount, cpuRatio, 0);
  }

  @Override
  @JsonIgnore
  public int getMinParallelizationWidth() {
    return FilterSpec.parititonCount(filters);
  }

  /**
   * The max parallelization width should be thought of as the
   * "minor fragment count." {@link #getScanStats()} will be called
   * once for each minor fragment specified here.
   */
  @Override
  @JsonIgnore
  public int getMaxParallelizationWidth() {
    return FilterSpec.parititonCount(filters);
  }

  /**
   * Convert the group scan into a physical scan description
   * (the so-called "sub scan"). Generally scans one file block,
   * one file, or so on. Here, we use the filter push-down to
   * specify "shards" which we will (pretend) to scan.
   *
   * @param minorFragmentId minor fragment id from 0 to the
   * number returned from {{@link #getMaxAllocation()}
   */
  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    Preconditions.checkArgument(minorFragmentId < endpointCount);
    if (!hasFilters()) {
      Preconditions.checkState(minorFragmentId == 0);
      return new DummySubScan(this, null);
    }
    int orCount = filters.partitionCount();
    int sliceSize = orCount / endpointCount;
    List<List<RelOp>> segmentFilters = new ArrayList<>();
    int start = minorFragmentId * sliceSize;
    int end = Math.min(start + sliceSize, orCount);
    for (int i = start; i < end; i++) {
      segmentFilters.add(filters.distribute(i));
    }
    return new DummySubScan(this, segmentFilters);
  }

  @Override
  public void buildPlanString(PlanStringBuilder builder) {
    super.buildPlanString(builder);
    builder.field("scanSpec", scanSpec);
    builder.field("filters", filters);
  }
}
