/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.sql.conversion;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.planner.sql.SchemaUtilities;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.security.ranger.AccessAuthorizer;
import org.apache.drill.exec.security.ranger.AccessAuthorizerFactory;
import org.apache.ranger.authorization.drill.resource.DrillAccessType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Visitor that traverses a RelNode tree and enforces column-level SELECT authorization
 * via the configured {@link AccessAuthorizer} (Ranger by default).
 *
 * <p><b>Design:</b> For every RelNode that carries {@link RexNode} expressions
 * (Project, Filter, Join, Aggregate, Sort), the visitor collects all {@link RexInputRef}s
 * and uses Calcite's {@link RelMetadataQuery#getColumnOrigins(RelNode, int)} to trace each
 * referenced column back to its originating {@link TableScan} column index. This correctly
 * handles multi-hop projections, filters, joins, and aggregations.</p>
 *
 * <p>For a {@link TableScan} that has NO traced column references (e.g.
 * {@code SELECT * FROM t} with no intervening Project), ALL columns of that table
 * are checked.</p>
 *
 * <p>System schemas (INFORMATION_SCHEMA, sys) are bypassed inside the authorizer
 * implementation. When authorization is disabled, the visitor is a no-op (fail-open).</p>
 */
class ColumnAccessChecker extends RelShuttleImpl {

  private static final Logger logger = LoggerFactory.getLogger(ColumnAccessChecker.class);

  private final UserSession session;
  private final DrillConfig drillConfig;
  private final RelMetadataQuery mq;

  // Records each table's referenced column indices. Uses IdentityHashMap because
  // RelOptTable equals/hashCode may be expensive or not identity-based.
  private final Map<RelOptTable, Set<Integer>> tableToReferencedCols = new IdentityHashMap<>();

  ColumnAccessChecker(UserSession session, DrillConfig drillConfig, RelMetadataQuery mq) {
    this.session = session;
    this.drillConfig = drillConfig;
    this.mq = mq;
  }

  /**
   * Entry point: traverse the tree and enforce column-level access.
   */
  void check(RelNode root) {
    // Also trace the root node's output columns (covers bare TableScan root or
    // top-level Project output).
    traceOutputColumns(root);

    // Walk the tree to collect RexInputRef origins from all expression-bearing nodes.
    root.accept(this);
  }

  // ------------------------------------------------------------------
  // RelShuttle overrides — collect RexInputRefs from expression-bearing nodes
  // ------------------------------------------------------------------

  @Override
  public RelNode visit(LogicalProject project) {
    collectRefs(project.getProjects(), project.getInput());
    return super.visit(project);
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    if (filter.getCondition() != null) {
      analyzeRex(filter.getCondition(), filter.getInput(), -1, null);
    }
    return super.visit(filter);
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    if (join.getCondition() != null) {
      int leftCount = join.getLeft().getRowType().getFieldCount();
      analyzeRex(join.getCondition(), join.getRight(), leftCount, join.getLeft());
    }
    return super.visit(join);
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    for (int i : aggregate.getGroupSet()) {
      traceColumnOrigin(aggregate.getInput(), i);
    }
    return super.visit(aggregate);
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    if (sort.getCollation() != null) {
      sort.getCollation().getFieldCollations().forEach(fc ->
          traceColumnOrigin(sort.getInput(), fc.getFieldIndex()));
    }
    return super.visit(sort);
  }

  @Override
  public RelNode visit(TableScan scan) {
    RelOptTable table = scan.getTable();

    // Determine which columns to check: traced columns, or ALL if none were traced
    // (SELECT * FROM t case).
    Set<Integer> referencedColIndices = tableToReferencedCols.get(table);
    List<String> allColumnNames = scan.getRowType().getFieldNames();

    Set<String> columnsToCheck;
    if (referencedColIndices == null || referencedColIndices.isEmpty()) {
      // SELECT * — check all columns
      columnsToCheck = new HashSet<>(allColumnNames);
    } else {
      columnsToCheck = new HashSet<>();
      for (int idx : referencedColIndices) {
        if (idx >= 0 && idx < allColumnNames.size()) {
          columnsToCheck.add(allColumnNames.get(idx));
        }
      }
    }

    if (columnsToCheck.isEmpty()) {
      return scan;
    }

    enforceColumnAccess(table, columnsToCheck);
    return scan;
  }

  /**
   * Traces the output columns of a RelNode back to their table-scan origins.
   */
  private void traceOutputColumns(RelNode node) {
    if (node == null) {
      return;
    }
    int fieldCount = node.getRowType().getFieldCount();
    for (int i = 0; i < fieldCount; i++) {
      traceColumnOrigin(node, i);
    }
  }

  /**
   * Traces a single output column of {@code node} at index {@code columnIndex} back to
   * table-scan origins, recording them in {@link #tableToReferencedCols}.
   */
  private void traceColumnOrigin(RelNode node, int columnIndex) {
    if (node == null || mq == null) {
      return;
    }
    Set<RelColumnOrigin> origins;
    try {
      origins = mq.getColumnOrigins(node, columnIndex);
    } catch (Exception e) {
      logger.debug("getColumnOrigins failed for {} column {}", node, columnIndex, e);
      return;
    }
    if (origins == null) {
      return;
    }
    for (RelColumnOrigin origin : origins) {
      RelOptTable originTable = origin.getOriginTable();
      if (originTable != null) {
        // Record origins for ALL table types (DrillTable, JdbcTable, etc.).
        // Previously this only recorded DrillTable origins, which caused
        // JDBC storage plugin tables (JdbcTable) to be skipped entirely.
        tableToReferencedCols
            .computeIfAbsent(originTable, k -> new HashSet<>())
            .add(origin.getOriginColumnOrdinal());
      }
    }
  }

  /**
   * Collects RexInputRefs from a list of RexNodes and traces each to its
   * table-scan origin via the input node's metadata. Also processes any
   * {@link RexSubQuery} found in the expressions (scalar/IN/EXISTS subqueries)
   * so that column references inside subqueries are authorized.
   */
  private void collectRefs(List<RexNode> rexNodes, RelNode inputNode) {
    if (rexNodes == null || inputNode == null) {
      return;
    }
    for (RexNode rex : rexNodes) {
      analyzeRex(rex, inputNode, -1, null);
    }
  }

  /**
   * Analyzes a {@link RexNode} expression, collecting {@link RexInputRef}s and
   * {@link RexSubQuery}s, tracing each input ref to its table-scan origin and
   * recursively visiting each subquery's {@link RelNode} tree.
   *
   * @param rex        the expression to analyze
   * @param inputNode  the input RelNode that RexInputRefs resolve against
   * @param leftCount  if {@code >= 0}, indicates a join condition: refs with
   *                   index {@code < leftCount} resolve against {@code leftInput},
   *                   others resolve against {@code inputNode} (the right input)
   *                   with offset {@code leftCount}. If {@code < 0}, all refs
   *                   resolve against {@code inputNode}.
   * @param leftInput  the left input of a join, or {@code null} when
   *                   {@code leftCount < 0}.
   */
  private void analyzeRex(RexNode rex, RelNode inputNode, int leftCount, RelNode leftInput) {
    if (rex == null) {
      return;
    }
    Set<Integer> refs = new HashSet<>();
    List<RexSubQuery> subQueries = new ArrayList<>();
    rex.accept(new RexRefCollector(refs, subQueries));
    for (int refIndex : refs) {
      if (leftCount >= 0 && refIndex < leftCount) {
        traceColumnOrigin(leftInput, refIndex);
      } else if (leftCount >= 0) {
        traceColumnOrigin(inputNode, refIndex - leftCount);
      } else {
        traceColumnOrigin(inputNode, refIndex);
      }
    }
    for (RexSubQuery sq : subQueries) {
      // Trace the subquery's output columns to their table-scan origins.
      // For scalar subqueries (e.g. SELECT sum(user_id) FROM t), this traces
      // the aggregate output back to the underlying table column.
      traceOutputColumns(sq.rel);
      // Recursively visit the subquery's RelNode tree so that RexInputRefs
      // inside the subquery (e.g. columns in WHERE/SELECT of the subquery)
      // are also collected and traced.
      sq.rel.accept(this);
    }
  }

  /**
   * Enforces column-level access for the given table and column set.
   */
  private void enforceColumnAccess(RelOptTable table, Set<String> columns) {
    AccessAuthorizer authorizer = AccessAuthorizerFactory.getAuthorizer(drillConfig);
    if (!authorizer.isEnabled()) {
      return; // fail-open when disabled
    }

    // Resolve datasource / schema / table from the qualified name, consistent
    // with DrillCalciteCatalogReader.checkTableAccess(). This works for ALL
    // table types (DrillTable, JdbcTable, etc.) — previously this method
    // required a DrillTable and skipped JdbcTable, leaving JDBC storage
    // plugin tables without column-level authorization.
    List<String> qualifiedName = table.getQualifiedName();
    String tableName = qualifiedName.get(qualifiedName.size() - 1);
    String dataSource;
    String schemaPath;
    if (qualifiedName.size() > 2) {
      dataSource = qualifiedName.get(0);
      schemaPath = SchemaUtilities.getSchemaPath(qualifiedName.subList(1, qualifiedName.size() - 1));
    } else if (qualifiedName.size() == 2) {
      dataSource = qualifiedName.get(0);
      schemaPath = DrillCalciteCatalogReader.getDefaultSchemaByDataSource(dataSource);
    } else {
      dataSource = tableName;
      schemaPath = DrillCalciteCatalogReader.getDefaultSchemaByDataSource(dataSource);
    }
    String userName = session.getCredentials().getUserName();

    if (!authorizer.checkColumnAccess(userName, dataSource, schemaPath, tableName, columns, DrillAccessType.SELECT.name())) {
      throw UserException.permissionError()
          .message("Access denied: user '%s' lacks SELECT privilege on one or more columns " +
              "(%s) of table %s.%s.%s", userName, columns, dataSource, schemaPath, tableName)
          .build(logger);
    }
  }

  /**
   * RexVisitor that collects all {@link RexInputRef} indices and
   * {@link RexSubQuery} instances encountered in a {@link RexNode} tree.
   *
   * <p>{@link RexSubQuery#accept(RexVisitor)} dispatches to
   * {@link #visitSubQuery(RexSubQuery)} (not {@code visitCall}), so a plain
   * {@code RexInputRef}-only visitor silently skips over subqueries. This
   * collector overrides {@code visitSubQuery} to capture the subquery and then
   * continues traversing its operands so that nested {@link RexInputRef}s
   * (e.g. the left side of {@code x IN (SELECT ...)}) and nested subqueries
   * are also collected.</p>
   */
  private static final class RexRefCollector extends RexVisitorImpl<Void> {
    private final Set<Integer> refs;
    private final List<RexSubQuery> subQueries;

    RexRefCollector(Set<Integer> refs, List<RexSubQuery> subQueries) {
      super(true);
      this.refs = refs;
      this.subQueries = subQueries;
    }

    @Override
    public Void visitInputRef(RexInputRef ref) {
      refs.add(ref.getIndex());
      return null;
    }

    @Override
    public Void visitSubQuery(RexSubQuery subQuery) {
      subQueries.add(subQuery);
      // Continue traversing operands (e.g. the left expression of `x IN (...)`)
      // to collect any RexInputRefs and nested RexSubQueries within them.
      for (RexNode operand : subQuery.getOperands()) {
        operand.accept(this);
      }
      return null;
    }
  }
}
