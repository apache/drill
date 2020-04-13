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
package org.apache.drill.exec.store.http;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.expression.SchemaPath;

import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;


@JsonTypeName("http-scan")
public class HttpGroupScan extends AbstractGroupScan {

  private final List<SchemaPath> columns;
  private final HttpScanSpec httpScanSpec;
  private final HttpStoragePluginConfig config;

  public HttpGroupScan (
    HttpStoragePluginConfig config,
    HttpScanSpec scanSpec,
    List<SchemaPath> columns) {
    super("no-user");
    this.config = config;
    this.httpScanSpec = scanSpec;
    this.columns = columns;
  }

  public HttpGroupScan(HttpGroupScan that) {
    super(that);
    config = that.config();
    httpScanSpec = that.httpScanSpec();
    columns = that.getColumns();
  }

  public HttpGroupScan(HttpGroupScan that, List<SchemaPath> columns) {
    super("no-user");
    this.columns = columns;
    this.config = that.config;
    this.httpScanSpec = that.httpScanSpec;
  }

  @JsonCreator
  public HttpGroupScan(
    @JsonProperty("config") HttpStoragePluginConfig config,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("httpScanSpec") HttpScanSpec httpScanSpec
  ) {
    super("no-user");
    this.config = config;
    this.columns = columns;
    this.httpScanSpec = httpScanSpec;
  }

  @JsonProperty("config")
  public HttpStoragePluginConfig config() { return config; }

  @JsonProperty("columns")
  public List<SchemaPath> columns() { return columns; }

  @JsonProperty("httpScanSpec")
  public HttpScanSpec httpScanSpec() { return httpScanSpec; }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    // No filter pushdowns yet, so this method does nothing
    return;
  }

  @Override
  @JsonIgnore
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    return new HttpSubScan(config, httpScanSpec, columns);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return new HttpGroupScan(this, columns);
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new HttpGroupScan(this);
  }

  @Override
  public ScanStats getScanStats() {
    int estRowCount = 10_000;
    int rowWidth = columns == null ? 200 : 100;
    int estDataSize = estRowCount * 200 * rowWidth;
    int estCpuCost = DrillCostBase.PROJECT_CPU_COST;
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT,
        estRowCount, estCpuCost, estDataSize);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("httpScanSpec", httpScanSpec)
      .field("columns", columns)
      .field("httpStoragePluginConfig", config)
      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(httpScanSpec, columns, config);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    HttpGroupScan other = (HttpGroupScan) obj;
    return Objects.equals(httpScanSpec, other.httpScanSpec())
      && Objects.equals(columns, other.columns())
      && Objects.equals(config, other.config());
  }
}
