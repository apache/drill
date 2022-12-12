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
package org.apache.drill.exec.store.drill.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlDialect;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.planner.sql.conversion.SqlConverter;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.List;

public class DrillGroupScan extends AbstractGroupScan {
  private static final double ROWS = 1e6;

  private final DrillStoragePluginConfig pluginConfig;
  private final DrillScanSpec scanSpec;

  @JsonCreator
  public DrillGroupScan(
      @JsonProperty("userName") String userName,
      @JsonProperty("pluginConfig") DrillStoragePluginConfig pluginConfig,
      @JsonProperty("scanSpec") DrillScanSpec scanSpec) {
    super(userName);
    this.pluginConfig = pluginConfig;
    this.scanSpec = scanSpec;
  }

  public DrillGroupScan(DrillGroupScan that) {
    super(that);
    this.pluginConfig = that.pluginConfig;
    this.scanSpec = that.scanSpec;
  }

  @JsonProperty("pluginConfig")
  public DrillStoragePluginConfig getPluginConfig() {
    return pluginConfig;
  }

  @JsonProperty("scanSpec")
  public DrillScanSpec getScanSpec() {
    return scanSpec;
  }

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    return new DrillSubScan(userName, pluginConfig, scanSpec.getQuery());
  }

  @JsonIgnore
  public SqlDialect getDialect() {
    return new SqlDialect(SqlDialect.EMPTY_CONTEXT
      .withIdentifierQuoteString(pluginConfig.getIdentifierQuoteString())
      .withConformance(SqlConverter.DRILL_CONFORMANCE)
      .withUnquotedCasing(Casing.UNCHANGED)
      .withQuotedCasing(Casing.UNCHANGED));
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return new DrillGroupScan(this);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    return new DrillGroupScan(this);
  }

  @Override
  public ScanStats getScanStats() {
    return new ScanStats(
      ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT,
      (long) Math.max(ROWS, 1),
      1,
      1);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("scanSpec", scanSpec)
      .toString();
  }

  @Override
  @JsonIgnore
  public List<SchemaPath> getColumns() {
    return super.getColumns();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DrillGroupScan that = (DrillGroupScan) o;

    return new EqualsBuilder()
      .append(getPluginConfig(), that.getPluginConfig())
      .append(getScanSpec(), that.getScanSpec())
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(getPluginConfig())
      .append(getScanSpec())
      .toHashCode();
  }
}
