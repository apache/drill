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
package org.apache.drill.exec.store.iceberg.plan;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.iceberg.IcebergGroupScan;
import org.apache.drill.exec.store.iceberg.IcebergGroupScan.IcebergGroupScanBuilder;
import org.apache.drill.exec.store.plan.PluginImplementor;
import org.apache.drill.exec.store.plan.rel.PluginFilterRel;
import org.apache.drill.exec.store.plan.rel.StoragePluginTableScan;
import org.apache.drill.test.BaseTest;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestIcebergPluginImplementor extends BaseTest {
  @Test
  public void testStackedFiltersRetainBothConditions() throws Exception {
    JavaTypeFactoryImpl types = new JavaTypeFactoryImpl();
    RexBuilder rex = new RexBuilder(types);
    RelDataType integer = types.createSqlType(SqlTypeName.INTEGER);
    RelDataType rowType = types.builder().add("id", integer).build();
    RelOptCluster cluster = mock(RelOptCluster.class);
    RelOptPlanner planner = mock(RelOptPlanner.class);
    when(cluster.getPlanner()).thenReturn(planner);
    when(cluster.getRexBuilder()).thenReturn(rex);
    when(planner.getContext()).thenReturn(Contexts.of(mock(PlannerSettings.class)));

    AtomicReference<LogicalExpression> condition = new AtomicReference<>();
    IcebergGroupScan scan = mock(IcebergGroupScan.class);
    IcebergGroupScanBuilder builder = mock(IcebergGroupScanBuilder.class);
    when(scan.getCondition()).thenAnswer(call -> condition.get());
    when(scan.toBuilder()).thenReturn(builder);
    when(builder.condition(any(LogicalExpression.class))).thenAnswer(call -> {
      condition.set(call.getArgument(0));
      return builder;
    });
    when(builder.build()).thenReturn(scan);

    StoragePluginTableScan input = mock(StoragePluginTableScan.class);
    when(input.getGroupScan()).thenReturn(scan);
    when(input.getCluster()).thenReturn(cluster);
    when(input.getRowType()).thenReturn(rowType);
    doAnswer(call -> {
      ((PluginImplementor) call.getArgument(0)).implement(input);
      return null;
    }).when(input).implement(any(PluginImplementor.class));

    RexNode id = rex.makeInputRef(integer, 0);
    PluginFilterRel lower = mock(PluginFilterRel.class);
    when(lower.getInput()).thenReturn(input);
    when(lower.getCluster()).thenReturn(cluster);
    when(lower.getRowType()).thenReturn(rowType);
    when(lower.getCondition()).thenReturn(rex.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
        id, rex.makeExactLiteral(BigDecimal.valueOf(2), integer)));
    doAnswer(call -> {
      ((PluginImplementor) call.getArgument(0)).implement(lower);
      return null;
    }).when(lower).implement(any(PluginImplementor.class));

    PluginFilterRel upper = mock(PluginFilterRel.class);
    when(upper.getInput()).thenReturn(lower);
    when(upper.getCluster()).thenReturn(cluster);
    when(upper.getCondition()).thenReturn(rex.makeCall(SqlStdOperatorTable.LESS_THAN,
        id, rex.makeExactLiteral(BigDecimal.valueOf(4), integer)));
    new IcebergPluginImplementor().implement(upper);

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    Evaluator evaluator = new Evaluator(schema.asStruct(),
        condition.get().accept(DrillExprToIcebergTranslator.INSTANCE, null));
    GenericRecord record = GenericRecord.create(schema);
    record.setField("id", 1);
    assertFalse(evaluator.eval(record));
    record.setField("id", 2);
    assertTrue(evaluator.eval(record));
    record.setField("id", 3);
    assertTrue(evaluator.eval(record));
    record.setField("id", 4);
    assertFalse(evaluator.eval(record));
  }
}
