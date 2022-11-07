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
package org.apache.drill.exec.store.delta.plan;

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.common.DrillLimitRelBase;
import org.apache.drill.exec.planner.logical.DrillConstExecutor;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.delta.DeltaGroupScan;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.plan.AbstractPluginImplementor;
import org.apache.drill.exec.store.plan.rel.PluginFilterRel;
import org.apache.drill.exec.store.plan.rel.PluginLimitRel;
import org.apache.drill.exec.store.plan.rel.PluginProjectRel;
import org.apache.drill.exec.store.plan.rel.StoragePluginTableScan;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DeltaPluginImplementor extends AbstractPluginImplementor {

  private DeltaGroupScan groupScan;

  @Override
  public void implement(StoragePluginTableScan scan) {
    groupScan = (DeltaGroupScan) scan.getGroupScan();
  }

  @Override
  public void implement(PluginFilterRel filter) throws IOException {
    visitChild(filter.getInput());

    RexNode condition = filter.getCondition();
    LogicalExpression expression = DrillOptiq.toDrill(
      new DrillParseContext(PrelUtil.getPlannerSettings(filter.getCluster().getPlanner())),
      filter.getInput(),
      condition);

    DrillConstExecutor executor = (DrillConstExecutor) filter.getCluster().getPlanner().getExecutor();
    PlannerSettings plannerSettings = filter.getCluster().getPlanner().getContext().unwrap(PlannerSettings.class);
    groupScan = Optional.ofNullable((DeltaGroupScan) groupScan.applyFilter(expression, executor.getUdfUtilities(),
      plannerSettings.functionImplementationRegistry, plannerSettings.getOptions()))
      .orElse(groupScan);
  }

  @Override
  public void implement(PluginProjectRel project) throws IOException {
    visitChild(project.getInput());

    DrillParseContext context = new DrillParseContext(PrelUtil.getPlannerSettings(project.getCluster().getPlanner()));
    RelNode input = project.getInput();

    List<SchemaPath> projects = project.getProjects().stream()
      .map(e -> (SchemaPath) DrillOptiq.toDrill(context, input, e))
      .collect(Collectors.toList());
    groupScan = groupScan.clone(projects);
  }

  @Override
  public boolean canImplement(Filter filter) {
    FilterFinder filterFinder = new FilterFinder();
    filter.getInput().accept(filterFinder);
    return filterFinder.getFilter() == null;
  }

  @Override
  public void implement(PluginLimitRel limit) throws IOException {
    visitChild(limit.getInput());
    int maxRecords = getArtificialLimit(limit);
    if (maxRecords >= 0) {
      groupScan = groupScan.applyLimit(maxRecords);
    }
  }

  @Override
  public boolean canImplement(DrillLimitRelBase limit) {
    if (hasPluginGroupScan(limit)) {
      FirstLimitFinder finder = new FirstLimitFinder();
      limit.getInput().accept(finder);
      int oldLimit = getArtificialLimit(finder.getFetch(), finder.getOffset());
      int newLimit = getArtificialLimit(limit);
      return newLimit >= 0 && (oldLimit < 0 || newLimit < oldLimit);
    }
    return false;
  }

  @Override
  public boolean artificialLimit() {
    return true;
  }

  @Override
  public boolean artificialFilter() {
    return true;
  }

  @Override
  protected Class<? extends StoragePlugin> supportedPlugin() {
    return FileSystemPlugin.class;
  }

  @Override
  public boolean splitProject(Project project) {
    return true;
  }

  @Override
  public boolean canImplement(Project project) {
    return hasPluginGroupScan(project);
  }

  @Override
  public GroupScan getPhysicalOperator() {
    return groupScan;
  }

  @Override
  protected boolean hasPluginGroupScan(RelNode node) {
    return findGroupScan(node) instanceof DeltaGroupScan;
  }

  private int rexLiteralIntValue(RexLiteral offset) {
    return ((BigDecimal) offset.getValue()).intValue();
  }

  private int getArtificialLimit(DrillLimitRelBase limit) {
    return getArtificialLimit(limit.getFetch(), limit.getOffset());
  }

  private int getArtificialLimit(RexNode fetch, RexNode offset) {
    int maxRows = -1;
    if (fetch != null) {
      maxRows = rexLiteralIntValue((RexLiteral) fetch);
      if (offset != null) {
        maxRows += rexLiteralIntValue((RexLiteral) offset);
      }
    }
    return maxRows;
  }

  private static class FilterFinder extends RelShuttleImpl {
    private RelNode filter;

    @Override
    public RelNode visit(LogicalFilter filter) {
      this.filter = filter;
      return filter;
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof Filter) {
        this.filter = other;
        return other;
      } else if (other instanceof RelSubset) {
        RelSubset relSubset = (RelSubset) other;
        Util.first(relSubset.getBest(), relSubset.getOriginal()).accept(this);
      }
      return super.visit(other);
    }

    public RelNode getFilter() {
      return filter;
    }
  }

  private static class FirstLimitFinder extends RelShuttleImpl {
    private RexNode fetch;

    private RexNode offset;

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof DrillLimitRelBase) {
        DrillLimitRelBase limitRelBase = (DrillLimitRelBase) other;
        fetch = limitRelBase.getFetch();
        offset = limitRelBase.getOffset();
        return other;
      } else if (other instanceof RelSubset) {
        RelSubset relSubset = (RelSubset) other;
        Util.first(relSubset.getBest(), relSubset.getOriginal()).accept(this);
      }
      return super.visit(other);
    }

    public RexNode getFetch() {
      return fetch;
    }

    public RexNode getOffset() {
      return offset;
    }
  }
}
