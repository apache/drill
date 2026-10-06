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
package org.apache.drill.exec.planner.sql;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.NlsString;
import org.apache.drill.exec.planner.sql.conversion.SqlBoundDynamicParam;

/** Extracts SQL literals into typed slots independently of cache eligibility. */
final class PlanCacheParameterizer extends SqlShuttle {
  private final List<SqlLiteral> literals = new ArrayList<>();
  private final List<String> types = new ArrayList<>();
  private boolean preserveProjection;

  static Candidate parameterize(SqlNode parsed) {
    Objects.requireNonNull(parsed, "parsed");
    PlanCacheParameterizer visitor = new PlanCacheParameterizer();
    SqlNode parameterized = parsed.accept(visitor);
    if (visitor.literals.isEmpty()) {
      parameterized = parsed;
    }
    return new Candidate(parameterized,
        Collections.unmodifiableList(visitor.literals),
        parameterized.toString() + visitor.types);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    int configurationOperand = configurationOperand(call);
    if (configurationOperand >= 0 && configurationOperand < call.operandCount()) {
      // These operands become function names, encodings or resolved plugin
      // references during conversion. They are not bindable expressions in
      // the physical plan, so keep their entire subtree in the template key.
      SqlCall parameterized = (SqlCall) call.clone(call.getParserPosition());
      for (int i = 0; i < call.operandCount(); i++) {
        if (i != configurationOperand) {
          parameterized.setOperand(i, visitNullable(call.operand(i)));
        }
      }
      return parameterized;
    }
    if ("COALESCE".equalsIgnoreCase(call.getOperator().getName())
        && call.getOperandList().stream()
        .anyMatch(operand -> operand instanceof SqlLiteral
            && ((SqlLiteral) operand).getTypeName() == SqlTypeName.NULL)) {
      // Calcite cannot infer a dynamic parameter's type alongside an untyped
      // NULL. Keep this call in the key; explicit casts still allow slots.
      return call;
    }
    if (call instanceof SqlJoin) {
      SqlJoin join = (SqlJoin) call;
      return new SqlJoin(join.getParserPosition(), visitNullable(join.getLeft()),
          join.isNaturalNode(), join.getJoinTypeNode(), visitNullable(join.getRight()),
          join.getConditionTypeNode(), visitNullable(join.getCondition()));
    }
    if (call.getKind() == SqlKind.ITEM && call.operand(1) instanceof SqlLiteral) {
      // A map/array key identifies a field in the input schema. Changing it can
      // change the scan projection and the HBase column used by pushdown.
      return call.getOperator().createCall(call.getParserPosition(),
          visitNullable(call.operand(0)), call.operand(1));
    }
    // These literals control rows, ordinals, window frames or operator
    // configuration. Keep them verbatim in the template key.
    if (call.getKind() == SqlKind.VALUES || call.getKind() == SqlKind.ORDER_BY
        || call.getKind() == SqlKind.OVER) {
      if (call instanceof SqlOrderBy) {
        SqlOrderBy orderBy = (SqlOrderBy) call;
        boolean previous = preserveProjection;
        preserveProjection = true;
        SqlNode query;
        try {
          query = orderBy.query.accept(this);
        } finally {
          preserveProjection = previous;
        }
        return new SqlOrderBy(call.getParserPosition(), query, orderBy.orderList,
            orderBy.offset, orderBy.fetch);
      }
      return call;
    }
    if (call instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) call;
      // Keep structural SQL operands verbatim while visiting expressions.
      boolean structuralProjection = preserveProjection
          || (select.getGroup() != null && !select.getGroup().isEmpty())
          || (select.getOrderList() != null && !select.getOrderList().isEmpty())
          || (select.getWindowList() != null && !select.getWindowList().isEmpty());
      return new SqlSelect(select.getParserPosition(),
          (SqlNodeList) select.getOperandList().get(0),
          structuralProjection ? select.getSelectList()
              : (SqlNodeList) visitNullable(select.getSelectList()),
          visitNullable(select.getFrom()), visitNullable(select.getWhere()),
          select.getGroup(), visitNullable(select.getHaving()),
          select.getWindowList(), visitNullable(select.getQualify()),
          select.getOrderList(), select.getOffset(), select.getFetch(),
          select.getHints());
    }
    return super.visit(call);
  }

  private static int configurationOperand(SqlCall call) {
    // Keep this list aligned with the value-dependent rewrites in DrillOptiq
    // and PreProcessLogicalRel; ordinary data operands still get slots.
    switch (call.getOperator().getName().toUpperCase(Locale.ROOT)) {
      case "DATE_PART":
      case "DATE_TRUNC":
      case "EXTRACT":
      case "TIMESTAMPDIFF":
      case "TRIM":
      case "HTTPREQUEST":
      case "HTTP_REQUEST":
        return 0;
      case "CONVERT_FROM":
      case "CONVERT_TO":
        return 1;
      case "LENGTH":
        return call.operandCount() == 2 ? 1 : -1;
      default:
        return -1;
    }
  }

  private SqlNode visitNullable(SqlNode node) {
    return node == null ? null : node.accept(this);
  }

  @Override
  public SqlNode visit(SqlLiteral literal) {
    if (literal.getValue() == null) {
      // A NULL without an inferred type is part of the template, not a slot.
      return literal;
    }
    Object value = literal.getValue();
    if (!(value instanceof BigDecimal) && !(value instanceof Boolean)
        && !(value instanceof NlsString)) {
      // Temporal literals, including unresolved DATE literals, and structural
      // literals such as EXTRACT's time unit stay in the template key.
      return literal;
    }
    int index = literals.size();
    literals.add(literal);
    // Precision and scale affect inferred types and must be part of the key.
    String shape = literal.getTypeName().name();
    if (value instanceof BigDecimal) {
      BigDecimal number = (BigDecimal) value;
      shape += ":" + number.precision() + ":" + number.scale();
    } else if (value instanceof NlsString) {
      NlsString string = (NlsString) value;
      shape += ":" + string.getValue().length() + ":" + string.getCharsetName()
          + ":" + string.getCollation();
    }
    types.add(shape);
    return new SqlBoundDynamicParam(index, literal.getParserPosition(), literal);
  }

  static final class Candidate {
    final SqlNode sql;
    final List<SqlLiteral> literals;
    final String template;

    Candidate(SqlNode sql, List<SqlLiteral> literals, String template) {
      this.sql = sql;
      this.literals = literals;
      this.template = template;
    }
  }
}
