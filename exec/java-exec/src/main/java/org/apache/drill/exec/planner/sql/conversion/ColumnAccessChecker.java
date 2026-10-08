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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.security.AccessAuthorizerManager;
import org.apache.drill.exec.security.TableAccessResource;
import org.apache.drill.exec.security.spi.AccessAuthorizer;
import org.apache.drill.exec.security.spi.AccessType;
import org.apache.drill.exec.security.spi.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
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
 * <p>Correlated subqueries are handled as well: a reference to an outer query
 * column from inside a correlated subquery is represented by Calcite as a
 * {@link RexFieldAccess} over a {@link RexCorrelVariable} (e.g. {@code $cor0.id}),
 * not as a {@link RexInputRef}. Each correlation variable carries a unique
 * name {@code $corN} assigned by {@code SqlToRelConverter}; we keep an exact
 * map from variable name to the enclosing {@link RelNode} row scope it refers
 * to. The map entry is populated the FIRST time we see a given {@code $corN}
 * during Rex traversal: we use {@code putIfAbsent(name, enclosingScope)} so
 * that subsequent encounters (including cross-nested skip-level references
 * from deeper subqueries to an outer {@code $cor0}) never overwrite the
 * original, correct binding. This avoids the ambiguity of row-type-based
 * heuristics: multiple enclosing scopes with identical schemas no longer
 * cause false positives or missed detections, and no scope stack is needed —
 * a single field tracks the innermost enclosing scope and is saved/restored
 * around each subquery entry via try/finally.</p>
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

  // The innermost enclosing row scope for correlated references. When we are
  // about to descend into a RexSubQuery's Rel tree, we save the previous value
  // and set this field to the outer-query inputNode that the subquery's
  // $cor* variables are relative to; the saved value is restored in a finally
  // block. Because variable → scope registration uses putIfAbsent in the Rex
  // collector, a single field is sufficient even for deep nesting: skip-level
  // references to an outer $corN reuse the binding that was established the
  // first time that variable was seen (at a shallower enclosing scope).
  private RelNode enclosingScope;

  // Exact, name-keyed scope mapping: $cor0 → outer row scope, $cor1 → outer
  // row scope, etc. Populated with putIfAbsent during Rex traversal so the
  // first occurrence of each $corN establishes a permanent binding. Lookup is
  // pure get(); a miss falls back (safely, in the over-approximate direction)
  // to enclosingScope and emits a warning so unusual plans remain diagnosable.
  private final Map<String, RelNode> correlationScopeByVar = new HashMap<>();

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
    // AggregateCall arguments (the x in SUM(x)), FILTER (WHERE ...) columns,
    // and WITHIN GROUP collation columns are not covered by getGroupSet().
    // Trace them explicitly rather than relying on an implicit Project beneath
    // the aggregate (plan shape can change across Calcite upgrades).
    for (AggregateCall call : aggregate.getAggCallList()) {
      for (int arg : call.getArgList()) {
        traceColumnOrigin(aggregate.getInput(), arg);
      }
      if (call.filterArg >= 0) {
        traceColumnOrigin(aggregate.getInput(), call.filterArg);
      }
      if (call.getCollation() != null) {
        for (RelFieldCollation fc : call.getCollation().getFieldCollations()) {
          traceColumnOrigin(aggregate.getInput(), fc.getFieldIndex());
        }
      }
    }
    return super.visit(aggregate);
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    if (sort.getCollation() != null) {
      sort.getCollation().getFieldCollations().forEach(fc ->
          traceColumnOrigin(sort.getInput(), fc.getFieldIndex()));
    }
    // LIMIT/OFFSET are RexNode expressions that may carry RexSubQuery or
    // RexInputRef (e.g. LIMIT (SELECT MAX(amount) FROM ...)). analyzeRex
    // descends into them; without it, subqueries reachable only from Sort
    // would never be column-checked.
    if (sort.fetch != null) {
      analyzeRex(sort.fetch, sort.getInput(), -1, null);
    }
    if (sort.offset != null) {
      analyzeRex(sort.offset, sort.getInput(), -1, null);
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
    List<CorrelFieldAccess> correlAccesses = new ArrayList<>();
    rex.accept(new RexRefCollector(refs, subQueries, correlAccesses));
    for (int refIndex : refs) {
      if (leftCount >= 0 && refIndex < leftCount) {
        traceColumnOrigin(leftInput, refIndex);
      } else if (leftCount >= 0) {
        traceColumnOrigin(inputNode, refIndex - leftCount);
      } else {
        traceColumnOrigin(inputNode, refIndex);
      }
    }
    // Resolve correlated outer-column references. A RexFieldAccess over a
    // RexCorrelVariable (e.g. $cor0.id inside a correlated subquery) always
    // refers to an ENCLOSING subquery's row scope, never to inputNode itself
    // (a reference to inputNode would be a plain RexInputRef).
    //
    // Registration happens inside RexRefCollector.visitFieldAccess with
    // putIfAbsent(varName, enclosingScope), so by the time we reach here the
    // scope for every $corN we just collected has been bound if possible.
    // Lookup is a direct map get(); the uncommon case of an unbound variable
    // falls back (safely over-approximating) to enclosingScope before giving
    // up — tracing an extra scope is harmless and keeps us on the secure side
    // of the check.
    for (CorrelFieldAccess cfa : correlAccesses) {
      RelNode scope = correlationScopeByVar.get(cfa.varName);
      if (scope != null) {
        traceColumnOrigin(scope, cfa.fieldIndex);
        continue;
      }
      if (enclosingScope != null) {
        logger.debug("Correlated reference '{}' was not pre-bound; "
            + "falling back to current enclosingScope for a safe over-approximation.",
            cfa.varName);
        traceColumnOrigin(enclosingScope, cfa.fieldIndex);
      } else {
        logger.warn("Unresolved correlated column reference '{}' (field index {}): "
            + "no enclosing correlation scope could be determined. Column-level "
            + "authorization may be incomplete for this reference.",
            cfa.varName, cfa.fieldIndex);
      }
    }
    for (RexSubQuery sq : subQueries) {
      // Trace the subquery's output columns to their table-scan origins.
      // For scalar subqueries (e.g. SELECT sum(user_id) FROM t), this traces
      // the aggregate output back to the underlying table column.
      traceOutputColumns(sq.rel);
      // The subquery's $cor* variables reference the rows produced by
      // inputNode (the input of the node whose expression contained this
      // RexSubQuery). Make inputNode the active enclosing scope while we
      // descend into sq.rel; save/restore the previous value so nesting and
      // skip-level references behave correctly. Registration of new $corN
      // names in correlationScopeByVar uses putIfAbsent, so a variable whose
      // binding was established at a shallower scope is never overwritten.
      RelNode prevEnclosing = enclosingScope;
      enclosingScope = inputNode;
      try {
        // Recursively visit the subquery's RelNode tree so that RexInputRefs
        // and correlated references inside the subquery are also collected
        // and traced.
        sq.rel.accept(this);
      } finally {
        enclosingScope = prevEnclosing;
      }
    }
  }

  /**
   * Enforces column-level access for the given table and column set.
   */
  private void enforceColumnAccess(RelOptTable table, Set<String> columns) {
    AccessAuthorizer authorizer = AccessAuthorizerManager.getAuthorizer(drillConfig);
    // When authorization is disabled the manager returns the NoOp authorizer,
    // which allows all access — no enabled/disabled branching needed here.

    // Resolve datasource / schema / table from the qualified name via the
    // shared resolver, so column-level checks address exactly the same
    // resource as the table-level checks in DrillCalciteCatalogReader. This
    // works for ALL table types (DrillTable, JdbcTable, etc.) — previously
    // this method required a DrillTable and skipped JdbcTable, leaving JDBC
    // storage plugin tables without column-level authorization.
    TableAccessResource resource = TableAccessResource.resolve(table.getQualifiedName());
    String userName = session.getCredentials().getUserName();

    if (!authorizer.checkColumnAccess(UserIdentity.of(userName), resource.getDataSource(),
        resource.getSchemaPath(), resource.getTable(), columns, AccessType.SELECT)) {
      throw UserException.permissionError()
          .message("Access denied: user '%s' lacks SELECT privilege on one or more columns " +
              "(%s) of table %s", userName, columns, resource)
          .build(logger);
    }
  }

  /**
   * Holder for a correlated outer-column reference captured during Rex
   * traversal: the variable name of the {@link RexCorrelVariable}
   * (e.g. {@code "$cor0"}, used to do an exact lookup of the enclosing row
   * scope via {@link #correlationScopeByVar}) and the index of the referenced
   * field within that row scope.
   */
  private static final class CorrelFieldAccess {
    final String varName;
    final int fieldIndex;

    CorrelFieldAccess(String varName, int fieldIndex) {
      this.varName = varName;
      this.fieldIndex = fieldIndex;
    }
  }

  /**
   * RexVisitor that collects all {@link RexInputRef} indices,
   * {@link RexSubQuery} instances, and correlated outer-column references
   * encountered in a {@link RexNode} tree. A correlated reference to an outer
   * query column appears as a {@link RexFieldAccess} over a
   * {@link RexCorrelVariable} (e.g. {@code $cor0.id}); such references are not
   * {@link RexInputRef}s and would be silently skipped by a plain
   * {@code RexInputRef}-only visitor, bypassing column-level authorization for
   * the referenced outer column.
   *
   * <p>This collector is non-static so it can eagerly register each {@code
   * $corN} in {@link #correlationScopeByVar} at the exact moment we first
   * encounter it during Rex traversal. Registration is
   * {@code putIfAbsent(name, enclosingScope)}, which guarantees we never
   * overwrite a variable that was already bound at a shallower nesting level
   * (which is exactly how skip-level references to outer variables stay
   * correctly bound).
   *
   * <p>{@link RexSubQuery#accept(RexVisitor)} dispatches to
   * {@link #visitSubQuery(RexSubQuery)} (not {@code visitCall}), so a plain
   * {@code RexInputRef}-only visitor silently skips over subqueries. This
   * collector overrides {@code visitSubQuery} to capture the subquery and then
   * continues traversing its operands so that nested {@link RexInputRef}s
   * (e.g. the left side of {@code x IN (SELECT ...)}) and nested subqueries
   * are also collected.</p>
   */
  private final class RexRefCollector extends RexVisitorImpl<Void> {
    private final Set<Integer> refs;
    private final List<RexSubQuery> subQueries;
    private final List<CorrelFieldAccess> correlAccesses;

    RexRefCollector(Set<Integer> refs, List<RexSubQuery> subQueries,
        List<CorrelFieldAccess> correlAccesses) {
      super(true);
      this.refs = refs;
      this.subQueries = subQueries;
      this.correlAccesses = correlAccesses;
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

    @Override
    public Void visitFieldAccess(RexFieldAccess fieldAccess) {
      // A RexFieldAccess over a RexCorrelVariable is how Calcite represents a
      // correlated reference to an outer query's column (e.g. $cor0.id inside
      // a subquery). Such references are not RexInputRefs and would otherwise
      // be silently skipped by this collector, bypassing column-level
      // authorization for the referenced outer column.
      RexNode refExpr = fieldAccess.getReferenceExpr();
      if (refExpr instanceof RexCorrelVariable) {
        RexCorrelVariable corVar = (RexCorrelVariable) refExpr;
        String varName = corVar.getName();
        // Eager, idempotent registration: putIfAbsent so the first
        // enclosing scope we saw this variable under wins, and any later
        // occurrences (possibly from a deeper nested scope after
        // enclosingScope has been overwritten) do not clobber the binding.
        if (enclosingScope != null) {
          correlationScopeByVar.putIfAbsent(varName, enclosingScope);
        }
        correlAccesses.add(new CorrelFieldAccess(varName,
            fieldAccess.getField().getIndex()));
      }
      // Preserve default descent so that non-correlated field accesses (e.g.
      // struct field access over a regular column) continue to be traversed
      // exactly as before this override.
      return super.visitFieldAccess(fieldAccess);
    }
  }
}
