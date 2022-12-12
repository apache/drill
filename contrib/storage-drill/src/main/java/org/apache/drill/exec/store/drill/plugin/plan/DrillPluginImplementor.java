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
package org.apache.drill.exec.store.drill.plugin.plan;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.common.DrillLimitRelBase;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.drill.plugin.DrillGroupScan;
import org.apache.drill.exec.store.drill.plugin.DrillScanSpec;
import org.apache.drill.exec.store.drill.plugin.DrillStoragePlugin;
import org.apache.drill.exec.store.plan.AbstractPluginImplementor;
import org.apache.drill.exec.store.plan.rel.PluginAggregateRel;
import org.apache.drill.exec.store.plan.rel.PluginFilterRel;
import org.apache.drill.exec.store.plan.rel.PluginJoinRel;
import org.apache.drill.exec.store.plan.rel.PluginLimitRel;
import org.apache.drill.exec.store.plan.rel.PluginProjectRel;
import org.apache.drill.exec.store.plan.rel.PluginSortRel;
import org.apache.drill.exec.store.plan.rel.PluginUnionRel;
import org.apache.drill.exec.store.plan.rel.StoragePluginTableScan;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class DrillPluginImplementor  extends AbstractPluginImplementor {
  private DrillGroupScan groupScan;
  private boolean isRoot = true;

  @Override
  protected Class<? extends StoragePlugin> supportedPlugin() {
    return DrillStoragePlugin.class;
  }

  @Override
  public void implement(StoragePluginTableScan scan) {
    groupScan = (DrillGroupScan) scan.getGroupScan();

    if (isRoot) {
      completeProcessing(scan);
    }
  }

  private void completeProcessing(RelNode scan) {
    String query = buildQuery(scan);
    DrillScanSpec scanSpec = new DrillScanSpec(query);
    groupScan = new DrillGroupScan(groupScan.getUserName(), groupScan.getPluginConfig(), scanSpec);
  }

  @Override
  public void implement(PluginAggregateRel aggregate) throws IOException {
    process(aggregate);
  }

  private void process(RelNode relNode) throws IOException {
    boolean isRoot = this.isRoot;
    this.isRoot = false;
    for (RelNode input : relNode.getInputs()) {
      visitChild(input);
    }

    if (isRoot) {
      completeProcessing(relNode);
    }
  }

  @Override
  public void implement(PluginFilterRel filter) throws IOException {
    process(filter);
  }

  @Override
  public void implement(PluginLimitRel limit) throws IOException {
    process(limit);
  }

  @Override
  public void implement(PluginProjectRel project) throws IOException {
    process(project);
  }

  @Override
  public void implement(PluginSortRel sort) throws IOException {
    process(sort);
  }

  @Override
  public void implement(PluginUnionRel union) throws IOException {
    process(union);
  }

  @Override
  public void implement(PluginJoinRel join) throws IOException {
    process(join);
  }

  @Override
  public boolean canImplement(Aggregate aggregate) {
    return true;
  }

  @Override
  public boolean canImplement(Filter filter) {
    return true;
  }

  @Override
  public boolean canImplement(DrillLimitRelBase limit) {
    return true;
  }

  @Override
  public boolean canImplement(Project project) {
    return true;
  }

  @Override
  public boolean canImplement(Sort sort) {
    return true;
  }

  @Override
  public boolean canImplement(Union union) {
    return true;
  }

  @Override
  public boolean canImplement(TableScan scan) {
    return true;
  }

  @Override
  public boolean canImplement(Join scan) {
    return true;
  }

  @Override
  protected boolean hasPluginGroupScan(RelNode node) {
    return findGroupScan(node) instanceof DrillGroupScan;
  }

  @Override
  public GroupScan getPhysicalOperator() {
    return groupScan;
  }

  public String buildQuery(RelNode node) {
    SqlDialect dialect = groupScan.getDialect();
    JdbcImplementor jdbcImplementor = new DrillRelToSqlConverter(dialect,
      (JavaTypeFactory) node.getCluster().getTypeFactory());
    JdbcImplementor.Result result = jdbcImplementor.visitRoot(node);
    return result.asStatement().toSqlString(dialect).getSql();
  }

  public static class DrillRelToSqlConverter extends JdbcImplementor {

    public DrillRelToSqlConverter(SqlDialect dialect, JavaTypeFactory typeFactory) {
      super(dialect, typeFactory);
    }

    @SuppressWarnings("unused")
    public Result visit(PluginLimitRel e) {
      Result x = visitInput(e, 0, Clause.OFFSET, Clause.FETCH);
      Builder builder = x.builder(e);
      Optional.ofNullable(e.getFetch())
        .ifPresent(fetch -> builder.setFetch(builder.context.toSql(null, fetch)));
      Optional.ofNullable(e.getOffset())
        .ifPresent(offset -> builder.setOffset(builder.context.toSql(null, offset)));
      return builder.result();
    }

    @Override
    public Result visit(TableScan scan) {
      List<String> qualifiedName = scan.getTable().getQualifiedName();
      SqlIdentifier sqlIdentifier = new SqlIdentifier(
        qualifiedName.subList(1, qualifiedName.size()), SqlParserPos.ZERO);
      return result(sqlIdentifier, ImmutableList.of(Clause.FROM), scan, null);
    }
  }
}
