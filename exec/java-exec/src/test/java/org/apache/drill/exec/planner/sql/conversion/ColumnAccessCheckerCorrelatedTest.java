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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.test.BaseTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Pure JUnit tests for {@link ColumnAccessChecker} focused on correlated
 * subquery outer-column references.
 *
 * <p>These tests DO NOT start a Drillbit, load any storage plugin, or require
 * Ranger to be enabled. They build a Calcite RelNode tree directly and feed
 * it to {@code ColumnAccessChecker.check()}. When Ranger is disabled (the
 * default), {@code AccessAuthorizerManager} returns a no-op authorizer, so no
 * real enforcement decision is made. Instead, we inspect the bookkeeping map
 * {@code ColumnAccessChecker.tableToReferencedCols} via reflection after the
 * visit completes. That map records origin table-column indices for every
 * column reference the checker discovered, including references traced
 * through correlated outer-column resolution.
 *
 * <p>Covered scenarios:</p>
 * <ol>
 *   <li>Simple correlated EXISTS referencing a column the outer SELECT does
 *     NOT project — the exact gap flagged by the reviewer at
 *     ColumnAccessChecker:323 (Doc case 13 masked it with {@code SELECT *}).</li>
 *   <li>Two nested EXISTS subqueries where the innermost does a skip-level
 *     correlation back to the outermost table — verifies
 *     {@code putIfAbsent(varName, enclosingScope)} semantics so an
 *     intermediate enclosing scope does not clobber the outer binding.</li>
 * </ol>
 */
public class ColumnAccessCheckerCorrelatedTest extends BaseTest {

  // Column indices for both test tables. The two tables share an identical
  // field layout (same field names at the same ordinal positions) so that any
  // row-type-based heuristic would be unable to distinguish them; an
  // exact-name binding has no ambiguity here.
  //
  //   orders:   [id(0 INT), amount(1 DECIMAL), user_id(2 INT), active(3 BOOLEAN)]
  //   customers:[id(0 INT), name(1   VARCHAR), user_id(2 INT)]
  private static final int COL_ID = 0;
  private static final int COL_USER_ID = 2;

  private RelOptCluster cluster;
  private RexBuilder rexBuilder;
  private SqlTypeFactoryImpl typeFactory;
  private RelOptTable ordersTable;
  private RelOptTable customersTable;

  @Before
  public void setUp() {
    typeFactory = new SqlTypeFactoryImpl(
        org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);

    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

    cluster = RelOptCluster.create(planner, rexBuilder);
    cluster.setMetadataQuerySupplier(RelMetadataQuery::instance);

    ordersTable = new StubRelOptTable(
        ImmutableList.of("cp", "default", "orders"),
        buildOrdersRowType(),
        typeFactory);
    customersTable = new StubRelOptTable(
        ImmutableList.of("cp", "default", "customers"),
        buildCustomersRowType(),
        typeFactory);
  }

  private RelDataType buildOrdersRowType() {
    return typeFactory.builder()
        .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("amount", typeFactory.createSqlType(SqlTypeName.DECIMAL))
        .add("user_id", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("active", typeFactory.createSqlType(SqlTypeName.BOOLEAN))
        .build();
  }

  private RelDataType buildCustomersRowType() {
    return typeFactory.builder()
        .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR, 64))
        .add("user_id", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();
  }

  @After
  public void tearDown() throws Exception {
    Field f = Class.forName(
        "org.apache.drill.exec.security.AccessAuthorizerManager")
        .getDeclaredField("instance");
    f.setAccessible(true);
    f.set(null, null);
  }

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  private Map<RelOptTable, Set<Integer>> runCheck(RelNode root) throws Exception {
    UserBitShared.UserCredentials creds = UserBitShared.UserCredentials.newBuilder()
        .setUserName("alice").build();
    UserSession session = Mockito.mock(UserSession.class);
    Mockito.when(session.getCredentials()).thenReturn(creds);

    DrillConfig drillConfig = DrillConfig.create();
    RelMetadataQuery mq = cluster.getMetadataQuery();
    ColumnAccessChecker checker = new ColumnAccessChecker(session, drillConfig, mq);
    // Bookkeeping map is populated before enforceColumnAccess is called, so
    // the default no-op authorizer is sufficient for our purposes.
    checker.check(root);

    Field f = ColumnAccessChecker.class.getDeclaredField("tableToReferencedCols");
    f.setAccessible(true);
    return (Map<RelOptTable, Set<Integer>>) f.get(checker);
  }

  private static void assertRefColumns(RelOptTable table,
      Map<RelOptTable, Set<Integer>> state,
      Integer... expectedIndices) {
    Set<Integer> actual = state.get(table);
    assertNotNull("Expected references for table " + table.getQualifiedName()
        + " but none were recorded", actual);
    Set<Integer> expected = new HashSet<>(Arrays.asList(expectedIndices));
    assertEquals("Referenced column indices for " + table.getQualifiedName()
            + " differ. expected=" + expected + " actual=" + actual,
        expected, actual);
  }

  private static void assertRefContains(RelOptTable table,
      Map<RelOptTable, Set<Integer>> state, int index) {
    Set<Integer> actual = state.get(table);
    assertNotNull("Expected references for " + table.getQualifiedName()
        + " but none were recorded", actual);
    assertTrue("Expected column index " + index
            + " to be recorded for " + table.getQualifiedName()
            + ", actual=" + actual,
        actual.contains(index));
  }

  private static RelDataType intLiteralRowType(SqlTypeFactoryImpl tf) {
    return tf.createStructType(
        Collections.singletonList(tf.createSqlType(SqlTypeName.INTEGER)),
        Collections.singletonList("$f0"));
  }

  // ------------------------------------------------------------------
  // Tests
  // ------------------------------------------------------------------

  /**
   * SQL shape:
   * <pre>
   *   SELECT o.id
   *   FROM orders o
   *   WHERE EXISTS (
   *       SELECT 1 FROM customers c
   *       WHERE c.user_id = o.user_id
   *   );
   * </pre>
   *
   * The outer SELECT projects only {@code o.id} (column 0). The inner
   * predicate references {@code o.user_id} (column 2) via a correlated
   * variable {@code $cor0.user_id}. Before the fix,
   * {@code RexFieldAccess(RexCorrelVariable)} was silently skipped so
   * orders.user_id would be missing from the traced column set — a classic
   * column-level bypass.
   */
  @Test
  public void correlatedExistsRefsUnprojectedOuterColumn() throws Exception {
    RelNode ordersScan = LogicalTableScan.create(cluster, ordersTable, Collections.emptyList());
    CorrelationId corr0Id = cluster.createCorrel();
    RelNode customersScan = LogicalTableScan.create(cluster, customersTable, Collections.emptyList());

    RexCorrelVariable cor0 = (RexCorrelVariable)
        rexBuilder.makeCorrel(ordersTable.getRowType(), corr0Id);
    RexNode outerUserId = rexBuilder.makeFieldAccess(cor0, COL_USER_ID);
    RexNode innerUserId = rexBuilder.makeInputRef(customersScan, COL_USER_ID);
    RexNode innerCond = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        innerUserId, outerUserId);
    RelNode filteredCustomers = LogicalFilter.create(customersScan, innerCond);
    RelNode innerProject = LogicalProject.create(filteredCustomers,
        Collections.emptyList(),
        Collections.singletonList(rexBuilder.makeExactLiteral(BigDecimal.ONE)),
        intLiteralRowType(typeFactory));

    RexSubQuery existsSq = RexSubQuery.exists(innerProject);
    RelNode filteredOrders = LogicalFilter.create(ordersScan, existsSq);

    RelNode outerProject = LogicalProject.create(filteredOrders,
        Collections.emptyList(),
        Collections.singletonList(rexBuilder.makeInputRef(filteredOrders, COL_ID)),
        typeFactory.createStructType(
            Collections.singletonList(typeFactory.createSqlType(SqlTypeName.INTEGER)),
            Collections.singletonList("id")));

    Map<RelOptTable, Set<Integer>> refs = runCheck(outerProject);

    // orders: id (0) from outer SELECT projection + user_id (2) from the
    // correlated predicate captured by the new visitFieldAccess handler.
    assertRefColumns(ordersTable, refs, COL_ID, COL_USER_ID);

    // customers: the inner filter used user_id (column 2).
    assertRefContains(customersTable, refs, COL_USER_ID);
  }

  /**
   * SQL shape:
   * <pre>
   *   SELECT o.id
   *   FROM orders o
   *   WHERE EXISTS (
   *       SELECT 1 FROM customers c
   *       WHERE c.id = o.id
   *         AND EXISTS (
   *             SELECT 1 FROM customers c2
   *             WHERE c2.user_id = o.user_id   -- skip-level correlation
   *         )
   *   );
   * </pre>
   *
   * The innermost subquery references the outermost scope's user_id column.
   * Middle and outer scopes share an identical row schema, so a row-type-based
   * scope resolver would be ambiguous. The exact-name binding implemented in
   * ColumnAccessChecker remembers the $corN → scope mapping captured at
   * first encounter and never overwrites it for skip-level reuses.
   */
  @Test
  public void nestedExistsSkipLevelCorrelation_preservesExactBinding() throws Exception {
    RelNode ordersScan = LogicalTableScan.create(cluster, ordersTable, Collections.emptyList());

    CorrelationId corr0Id = cluster.createCorrel();
    CorrelationId corr1Id = cluster.createCorrel();
    RexCorrelVariable cor0 = (RexCorrelVariable)
        rexBuilder.makeCorrel(ordersTable.getRowType(), corr0Id);

    // Innermost: c2.user_id = $cor0.user_id  (SKIP LEVEL — not $cor1)
    RelNode c2Scan = LogicalTableScan.create(cluster, customersTable, Collections.emptyList());
    RexNode c2UserId = rexBuilder.makeInputRef(c2Scan, COL_USER_ID);
    RexNode skipOuterUserId = rexBuilder.makeFieldAccess(cor0, COL_USER_ID);
    RexNode innerCond = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        c2UserId, skipOuterUserId);
    RelNode filteredC2 = LogicalFilter.create(c2Scan, innerCond);
    RelNode innerProject = LogicalProject.create(filteredC2,
        Collections.emptyList(),
        Collections.singletonList(rexBuilder.makeExactLiteral(BigDecimal.ONE)),
        intLiteralRowType(typeFactory));
    RexSubQuery innerExists = RexSubQuery.exists(innerProject);

    // Middle customers: c.id = $cor0.id  AND  EXISTS(...)
    RelNode cScan = LogicalTableScan.create(cluster, customersTable, Collections.emptyList());
    RexNode cId = rexBuilder.makeInputRef(cScan, COL_ID);
    RexNode outerIdRef = rexBuilder.makeFieldAccess(cor0, COL_ID);
    RexNode firstCond = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        cId, outerIdRef);
    RexNode middleCond = rexBuilder.makeCall(SqlStdOperatorTable.AND,
        firstCond, innerExists);
    RelNode filteredC = LogicalFilter.create(cScan, middleCond);
    RelNode middleProject = LogicalProject.create(filteredC,
        Collections.emptyList(),
        Collections.singletonList(rexBuilder.makeExactLiteral(BigDecimal.ONE)),
        intLiteralRowType(typeFactory));
    RexSubQuery outerExists = RexSubQuery.exists(middleProject);

    RelNode filteredOrders = LogicalFilter.create(ordersScan, outerExists);
    RelNode outerProject = LogicalProject.create(filteredOrders,
        Collections.emptyList(),
        Collections.singletonList(rexBuilder.makeInputRef(filteredOrders, COL_ID)),
        typeFactory.createStructType(
            Collections.singletonList(typeFactory.createSqlType(SqlTypeName.INTEGER)),
            Collections.singletonList("id")));

    Map<RelOptTable, Set<Integer>> refs = runCheck(outerProject);

    // orders: id (0) from outer SELECT + outer $cor0.id + skip-level
    // $cor0.user_id (column 2).
    assertRefColumns(ordersTable, refs, COL_ID, COL_USER_ID);

    // customers: id (0) from middle predicate + user_id (2) from innermost.
    assertRefContains(customersTable, refs, COL_ID);
    assertRefContains(customersTable, refs, COL_USER_ID);
  }

  /**
   * SQL shape:
   * <pre>
   *   SELECT SUM(amount) FILTER (WHERE active) FROM orders
   * </pre>
   *
   * The aggregate has no GROUP BY and one AggregateCall SUM(amount) with a
   * FILTER clause referencing the boolean column active (filterArg=3).
   * Before the fix, only {@code getGroupSet()} was traced (empty here), so
   * neither amount (argList) nor active (filterArg) would be recorded. The
   * fix traces {@code getAggCallList()} args, filterArg, and collation
   * explicitly.
   */
  @Test
  public void aggregateCallArgsAndFilterAreTraced() throws Exception {
    RelNode ordersScan = LogicalTableScan.create(cluster, ordersTable, Collections.emptyList());

    // SUM(amount): argList=[1], filterArg=3 (active BOOLEAN NOT NULL column)
    AggregateCall sumCall = AggregateCall.create(
        SqlStdOperatorTable.SUM,
        false,   // distinct
        false,   // approximate
        false,   // ignoreNulls
        ImmutableList.of(1),  // argList: amount
        3,       // filterArg: active (BOOLEAN NOT NULL)
        null,    // distinctKeys
        RelCollations.EMPTY,  // collation
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.DECIMAL), true),
        "SUM(amount)");

    RelNode aggregate = LogicalAggregate.create(ordersScan,
        ImmutableBitSet.of(),  // groupSet: empty (no GROUP BY)
        ImmutableList.of(ImmutableBitSet.of()),  // groupSets
        Collections.singletonList(sumCall));

    Map<RelOptTable, Set<Integer>> refs = runCheck(aggregate);

    // amount (1) from argList + active (3) from filterArg — both must be present
    assertRefContains(ordersTable, refs, 1);
    assertRefContains(ordersTable, refs, 3);
  }

  /**
   * SQL shape:
   * <pre>
   *   SELECT * FROM orders ORDER BY id LIMIT <expression referencing amount>
   * </pre>
   *
   * The LIMIT (fetch) expression is a RexNode that may carry column references
   * or subqueries. Before the fix, {@code visit(LogicalSort)} only traced the
   * ORDER BY collation and never called {@code analyzeRex} on fetch/offset,
   * so any column reachable only from LIMIT/OFFSET was silently skipped.
   */
  @Test
  public void sortFetchExpressionIsAnalyzed() throws Exception {
    RelNode ordersScan = LogicalTableScan.create(cluster, ordersTable, Collections.emptyList());

    // ORDER BY id (column 0); LIMIT references amount (column 1).
    // Without the fix, only column 0 (from collation) would be traced;
    // column 1 (from fetch) would be missed.
    RexNode fetch = rexBuilder.makeInputRef(ordersScan, 1);
    RelNode sort = LogicalSort.create(ordersScan,
        RelCollations.of(0),  // ORDER BY id
        fetch,
        null);  // no offset

    Map<RelOptTable, Set<Integer>> refs = runCheck(sort);

    // id (0) from collation + amount (1) from fetch — both must be present
    assertRefContains(ordersTable, refs, 0);
    assertRefContains(ordersTable, refs, 1);
  }

  // ------------------------------------------------------------------
  // Stub RelOptTable: avoids the finicky RelOptTableImpl factory overloads
  // while satisfying LogicalTableScan + RelMetadataQuery column-origin
  // tracing. Only the methods reached by ColumnAccessChecker +
  // LogicalTableScan + MQ.getColumnOrigins are implemented meaningfully.
  // ------------------------------------------------------------------

  private static final class StubRelOptTable implements RelOptTable {
    private final ImmutableList<String> qualifiedName;
    private final RelDataType rowType;
    private final SqlTypeFactoryImpl tf;

    StubRelOptTable(ImmutableList<String> qualifiedName, RelDataType rowType,
        SqlTypeFactoryImpl tf) {
      this.qualifiedName = qualifiedName;
      this.rowType = rowType;
      this.tf = tf;
    }

    @Override public List<String> getQualifiedName() { return qualifiedName; }
    @Override public double getRowCount() { return 100; }
    @Override public RelDataType getRowType() { return rowType; }
    @Override public RelOptSchema getRelOptSchema() { return null; }
    @Override public RelNode toRel(ToRelContext context) {
      return LogicalTableScan.create(context.getCluster(), this, Collections.emptyList());
    }
    @Override public List<RelCollation> getCollationList() { return ImmutableList.of(); }
    @Override public RelDistribution getDistribution() { return null; }
    @Override public boolean isKey(ImmutableBitSet columns) { return false; }
    @Override public List<ImmutableBitSet> getKeys() { return ImmutableList.of(); }
    @Override public List<RelReferentialConstraint> getReferentialConstraints() {
      return ImmutableList.of();
    }
    @Override public Expression getExpression(Class clazz) { return null; }
    @Override public RelOptTable extend(List<RelDataTypeField> extendedFields) {
      RelDataTypeFactory.Builder b = tf.builder();
      for (RelDataTypeField f : rowType.getFieldList()) { b.add(f.getName(), f.getType()); }
      for (RelDataTypeField f : extendedFields) { b.add(f.getName(), f.getType()); }
      return new StubRelOptTable(qualifiedName, b.build(), tf);
    }
    @Override public List<ColumnStrategy> getColumnStrategies() {
      ImmutableList.Builder<ColumnStrategy> b = ImmutableList.builder();
      for (int i = 0; i < rowType.getFieldCount(); i++) { b.add(ColumnStrategy.NULLABLE); }
      return b.build();
    }
    @Override public <C> C unwrap(Class<C> aClass) {
      if (aClass.isInstance(this)) { return aClass.cast(this); }
      return null;
    }
  }
}
