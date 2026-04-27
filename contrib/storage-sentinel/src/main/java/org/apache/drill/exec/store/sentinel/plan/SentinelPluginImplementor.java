package org.apache.drill.exec.store.sentinel.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.planner.common.DrillLimitRelBase;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.plan.AbstractPluginImplementor;
import org.apache.drill.exec.store.plan.rel.PluginAggregateRel;
import org.apache.drill.exec.store.plan.rel.PluginFilterRel;
import org.apache.drill.exec.store.plan.rel.PluginLimitRel;
import org.apache.drill.exec.store.plan.rel.PluginProjectRel;
import org.apache.drill.exec.store.plan.rel.PluginSortRel;
import org.apache.drill.exec.store.plan.rel.StoragePluginTableScan;
import org.apache.drill.exec.store.sentinel.SentinelGroupScan;
import org.apache.drill.exec.store.sentinel.SentinelScanSpec;
import org.apache.drill.exec.store.sentinel.SentinelStoragePluginConfig;
import org.apache.drill.exec.store.sentinel.SentinelStoragePlugin;
import org.apache.drill.exec.physical.base.GroupScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SentinelPluginImplementor extends AbstractPluginImplementor {
  private static final Logger logger = LoggerFactory.getLogger(SentinelPluginImplementor.class);

  private StringBuilder kqlQuery;
  private SentinelGroupScan groupScan;
  private SentinelStoragePluginConfig config;
  private SentinelScanSpec scanSpec;
  private List<String> projectedColumns;

  @Override
  public boolean canImplement(TableScan scan) {
    return hasPluginGroupScan(scan);
  }

  @Override
  public boolean canImplement(Filter filter) {
    return hasPluginGroupScan(filter);
  }

  @Override
  public boolean canImplement(Project project) {
    return hasPluginGroupScan(project);
  }

  @Override
  public boolean canImplement(Aggregate aggregate) {
    if (!hasPluginGroupScan(aggregate)) {
      return false;
    }
    if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
      return false;
    }
    return aggregate.getAggCallList().stream()
        .allMatch(call -> isSupportedAggregateFunction(call.getAggregation().getKind()));
  }

  @Override
  public boolean canImplement(Sort sort) {
    return hasPluginGroupScan(sort);
  }

  @Override
  public boolean canImplement(DrillLimitRelBase limit) {
    return hasPluginGroupScan(limit);
  }

  @Override
  public void implement(StoragePluginTableScan scan) throws IOException {
    groupScan = (SentinelGroupScan) scan.getGroupScan();
    config = groupScan.getConfig();
    scanSpec = groupScan.getScanSpec();
    kqlQuery = new StringBuilder(scanSpec.getTableName());
    projectedColumns = new ArrayList<>();
  }

  @Override
  public void implement(PluginFilterRel filter) throws IOException {
    visitChild(filter.getInput());
    String kqlCondition = RexToKqlConverter.convert(filter.getCondition(), filter.getInput().getRowType());
    kqlQuery.append("\n| where ").append(kqlCondition);
  }

  public void implement(Filter filter) throws IOException {
    visitChild(filter.getInput());
    String kqlCondition = RexToKqlConverter.convert(filter.getCondition(), filter.getInput().getRowType());
    kqlQuery.append("\n| where ").append(kqlCondition);
  }

  @Override
  public void implement(PluginProjectRel project) throws IOException {
    visitChild(project.getInput());

    projectedColumns = new ArrayList<>();
    for (int i = 0; i < project.getProjects().size(); i++) {
      String colName = project.getRowType().getFieldList().get(i).getName();
      projectedColumns.add(colName);
    }

    if (!projectedColumns.isEmpty()) {
      String projectList = String.join(", ", projectedColumns);
      kqlQuery.append("\n| project ").append(projectList);
    }
  }

  @Override
  public void implement(PluginAggregateRel aggregate) throws IOException {
    visitChild(aggregate.getInput());

    ImmutableBitSet groupSet = aggregate.getGroupSet();
    List<String> groupCols = new ArrayList<>();

    for (int idx : groupSet.asList()) {
      String colName = aggregate.getInput().getRowType().getFieldList().get(idx).getName();
      groupCols.add(colName);
    }

    List<String> aggCalls = new ArrayList<>();
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      String aggExpr = buildAggregateExpression(aggregate, i);
      if (aggExpr != null) {
        aggCalls.add(aggExpr);
      }
    }

    StringBuilder summarize = new StringBuilder("| summarize ");
    if (!aggCalls.isEmpty()) {
      summarize.append(String.join(", ", aggCalls));
    }
    if (!groupCols.isEmpty()) {
      summarize.append(" by ").append(String.join(", ", groupCols));
    }

    kqlQuery.append("\n").append(summarize);
  }

  @Override
  public void implement(PluginSortRel sort) throws IOException {
    visitChild(sort.getInput());

    StringBuilder sortBuilder = new StringBuilder("| sort by ");
    List<String> sortItems = new ArrayList<>();

    for (int i = 0; i < sort.getCollation().getFieldCollations().size(); i++) {
      int colIdx = sort.getCollation().getFieldCollations().get(i).getFieldIndex();
      String colName = sort.getInput().getRowType().getFieldList().get(colIdx).getName();
      boolean isDesc = sort.getCollation().getFieldCollations().get(i).direction.isDescending();
      sortItems.add(colName + (isDesc ? " desc" : ""));
    }

    sortBuilder.append(String.join(", ", sortItems));
    kqlQuery.append("\n").append(sortBuilder);
  }

  @Override
  public void implement(PluginLimitRel limit) throws IOException {
    visitChild(limit.getInput());

    if (limit.getFetch() != null && limit.getFetch() instanceof RexLiteral) {
      long limitValue = ((RexLiteral) limit.getFetch()).getValueAs(Long.class);
      kqlQuery.append("\n| take ").append(limitValue);
    }
  }

  @Override
  public GroupScan getPhysicalOperator() throws IOException {
    SentinelScanSpec newSpec = new SentinelScanSpec(
        scanSpec.getPluginName(),
        scanSpec.getTableName(),
        kqlQuery.toString());

    return new SentinelGroupScan(config, newSpec, (org.apache.drill.exec.metastore.MetadataProviderManager) null);
  }

  @Override
  public Class<? extends StoragePlugin> supportedPlugin() {
    return SentinelStoragePlugin.class;
  }

  @Override
  public boolean hasPluginGroupScan(RelNode node) {
    SentinelGroupScan scan = (SentinelGroupScan) findGroupScan(node);
    return scan != null;
  }

  private String buildAggregateExpression(PluginAggregateRel aggregate, int aggIndex) {
    org.apache.calcite.rel.core.AggregateCall aggCall = aggregate.getAggCallList().get(aggIndex);
    SqlAggFunction aggFunc = aggCall.getAggregation();
    String outputName = aggregate.getRowType().getFieldList().get(aggregate.getGroupCount() + aggIndex).getName();

    switch (aggFunc.getKind()) {
      case COUNT:
        if (aggCall.getArgList().isEmpty()) {
          return "count() as " + outputName;
        } else {
          int argIdx = aggCall.getArgList().get(0);
          String colName = aggregate.getInput().getRowType().getFieldList().get(argIdx).getName();
          return "count(" + colName + ") as " + outputName;
        }
      case SUM:
      case SUM0:
        int sumIdx = aggCall.getArgList().get(0);
        String sumCol = aggregate.getInput().getRowType().getFieldList().get(sumIdx).getName();
        return "sum(" + sumCol + ") as " + outputName;
      case MIN:
        int minIdx = aggCall.getArgList().get(0);
        String minCol = aggregate.getInput().getRowType().getFieldList().get(minIdx).getName();
        return "min(" + minCol + ") as " + outputName;
      case MAX:
        int maxIdx = aggCall.getArgList().get(0);
        String maxCol = aggregate.getInput().getRowType().getFieldList().get(maxIdx).getName();
        return "max(" + maxCol + ") as " + outputName;
      case AVG:
        int avgIdx = aggCall.getArgList().get(0);
        String avgCol = aggregate.getInput().getRowType().getFieldList().get(avgIdx).getName();
        return "avg(" + avgCol + ") as " + outputName;
      default:
        logger.warn("Unsupported aggregate function: {}", aggFunc.getKind());
        return null;
    }
  }

  private boolean isSupportedAggregateFunction(SqlKind kind) {
    switch (kind) {
      case COUNT:
      case SUM:
      case SUM0:
      case MIN:
      case MAX:
      case AVG:
        return true;
      default:
        return false;
    }
  }
}
