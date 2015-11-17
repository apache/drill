/**
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
package org.apache.drill.exec.planner.sql.handlers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.planner.logical.DrillConstExecutor;
import org.apache.drill.exec.planner.logical.DrillLimitRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillValuesRel;

import java.math.BigDecimal;
import java.util.List;

/**
 * Visitor that will identify whether the root portion of the RelNode tree contains a limit 0 pattern. In this case, we
 * inform the planner settings that this plan should be run as a single node plan to reduce the overhead associated with
 * executing a schema-only query.
 */
public class FindLimit0Visitor extends RelShuttleImpl {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FindLimit0Visitor.class);

  /**
   * Values in the {@link DrillConstExecutor#DRILL_TO_CALCITE_TYPE_MAPPING} map.
   * + without {@link SqlTypeName#ANY} (avoid late binding)
   * + without {@link SqlTypeName#VARBINARY} ({@link DrillValuesRel values} does not support this)
   * + with {@link SqlTypeName#CHAR} ({@link DrillValuesRel values} supports this, but the above mapping does not
   *   contain this type.
   */
  private static final ImmutableSet<SqlTypeName> TYPES = ImmutableSet.<SqlTypeName>builder()
      .add(SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.FLOAT, SqlTypeName.DOUBLE, SqlTypeName.VARCHAR,
          SqlTypeName.BOOLEAN, SqlTypeName.DATE, SqlTypeName.DECIMAL, SqlTypeName.TIME, SqlTypeName.TIMESTAMP,
          SqlTypeName.INTERVAL_YEAR_MONTH, SqlTypeName.INTERVAL_DAY_TIME, SqlTypeName.MAP, SqlTypeName.ARRAY,
          SqlTypeName.TINYINT, SqlTypeName.SMALLINT, SqlTypeName.CHAR)
      .build();

  private boolean contains = false;

  /**
   * Checks if the root portion of the RelNode tree contains a limit 0 pattern.
   */
  public static boolean containsLimit0(RelNode rel) {
    FindLimit0Visitor visitor = new FindLimit0Visitor();
    rel.accept(visitor);
    return visitor.isContains();
  }

  /**
   * If all field types of the given node are {@link #TYPES recognized types}, then this method returns the tree:
   *   DrillLimitRel(0)
   *     \
   *     DrillValuesRel(field types)
   * Otherwise, the method returns null.
   */
  public static DrillRel getValuesRelIfFullySchemaed(final RelNode rel) {
    final List<RelDataTypeField> fieldList = rel.getRowType().getFieldList();
    final ImmutableList.Builder<RexLiteral> tupleBuilder = new ImmutableList.Builder<>();
    final RexBuilder literalBuilder = new RexBuilder(rel.getCluster().getTypeFactory());
    for (final RelDataTypeField field : fieldList) {
      if (!TYPES.contains(field.getType().getSqlTypeName())) {
        return null;
      } else {
        tupleBuilder.add((RexLiteral) literalBuilder.makeLiteral(null, field.getType(), false));
      }
    }

    final ImmutableList<ImmutableList<RexLiteral>> tuples = new ImmutableList.Builder<ImmutableList<RexLiteral>>()
        .add(tupleBuilder.build())
        .build();
    final RelTraitSet traits = rel.getTraitSet().plus(DrillRel.DRILL_LOGICAL);
    // TODO: ideally, we want the limit to be pushed into values
    final DrillValuesRel values = new DrillValuesRel(rel.getCluster(), rel.getRowType(), tuples, traits);
    return new DrillLimitRel(rel.getCluster(), traits, values, literalBuilder.makeExactLiteral(BigDecimal.ZERO),
        literalBuilder.makeExactLiteral(BigDecimal.ZERO));
  }

  private FindLimit0Visitor() {
  }

  boolean isContains() {
    return contains;
  }

  private boolean isLimit0(RexNode fetch) {
    if (fetch != null && fetch.isA(SqlKind.LITERAL)) {
      RexLiteral l = (RexLiteral) fetch;
      switch (l.getTypeName()) {
      case BIGINT:
      case INTEGER:
      case DECIMAL:
        if (((long) l.getValue2()) == 0) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    if (isLimit0(sort.fetch)) {
      contains = true;
      return sort;
    }

    return super.visit(sort);
  }

  // The following set of RelNodes should terminate a search for the limit 0 pattern as they want convey its meaning.

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    return aggregate;
  }

  @Override
  public RelNode visit(LogicalIntersect intersect) {
    return intersect;
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    return join;
  }

  @Override
  public RelNode visit(LogicalMinus minus) {
    return minus;
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    return union;
  }
}
