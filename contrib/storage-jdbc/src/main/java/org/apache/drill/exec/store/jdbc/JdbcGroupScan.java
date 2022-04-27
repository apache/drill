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
package org.apache.drill.exec.store.jdbc;

import java.util.List;
import java.util.Objects;

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("jdbc-scan")
public class JdbcGroupScan extends AbstractGroupScan {

  private final String sql;
  private final List<SchemaPath> columns;
  private final JdbcStoragePlugin plugin;
  private final double rows;
  private int hashCode;

  @JsonCreator
  public JdbcGroupScan(
      @JsonProperty("sql") String sql,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("config") JdbcStorageConfig config,
      @JsonProperty("rows") double rows,
      @JsonProperty("username") String username,
      @JacksonInject StoragePluginRegistry plugins) throws ExecutionSetupException {
    super(username);
    this.sql = sql;
    this.columns = columns;
    this.plugin = plugins.resolve(config, JdbcStoragePlugin.class);
    this.rows = rows;
  }

  JdbcGroupScan(String sql, List<SchemaPath> columns, JdbcStoragePlugin plugin, double rows, String username) {
    super(username);
    this.sql = sql;
    this.columns = columns;
    this.plugin = plugin;
    this.rows = rows;
  }

  @JsonProperty("config")
  public JdbcStorageConfig config() {
    return plugin.getConfig();
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    return new JdbcSubScan(sql, columns, plugin, getUserName());
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public ScanStats getScanStats() {
    return new ScanStats(
        GroupScanProperty.NO_EXACT_ROW_COUNT,
        (long) Math.max(rows, 1),
        1,
        1);
  }

  @JsonProperty("sql")
  public String getSql() {
    return sql;
  }

  @Override
  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public String getDigest() {
    return sql + plugin.getConfig();
  }

  public JdbcStorageConfig getConfig() {
    return plugin.getConfig();
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return new JdbcGroupScan(sql, columns, plugin, rows, userName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    JdbcGroupScan that = (JdbcGroupScan) obj;
    return Objects.equals(sql, that.sql) &&
      Objects.equals(columns, that.columns) &&
      Objects.equals(rows, that.rows) &&
      Objects.equals(plugin.getName(), that.plugin.getName()) &&
      Objects.equals(config(), that.getConfig());
  }

  @Override
  public int hashCode() {
    // Hash code is cached since Calcite calls this method many times.
    if (hashCode == 0) {
      // Don't include cost; it is derived.
      hashCode = Objects.hash(sql, columns, plugin.getConfig(), rows, plugin.getName());
    }
    return hashCode;
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("sql", sql)
      .field("columns", columns)
      .field("jdbcConfig", plugin.getConfig())
      .field("rows", rows)
      .toString();
  }

}
