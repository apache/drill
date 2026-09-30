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

package org.apache.drill.exec.store.sentinel.plan;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.exceptions.UserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RexToKqlConverter extends RexVisitorImpl<String> {
  private static final Logger logger = LoggerFactory.getLogger(RexToKqlConverter.class);

  private final RelDataType rowType;

  public RexToKqlConverter(RelDataType rowType) {
    super(true);
    this.rowType = rowType;
  }

  public static String convert(RexNode node, RelDataType rowType) {
    return node.accept(new RexToKqlConverter(rowType));
  }

  @Override
  public String visitInputRef(RexInputRef inputRef) {
    if (rowType != null && inputRef.getIndex() < rowType.getFieldCount()) {
      return rowType.getFieldList().get(inputRef.getIndex()).getName();
    }
    return "col" + inputRef.getIndex();
  }

  @Override
  public String visitLiteral(RexLiteral literal) {
    if (literal.isNull()) {
      return "null";
    }

    SqlTypeName typeName = literal.getType().getSqlTypeName();
    Object value = literal.getValue();

    switch (typeName) {
      case VARCHAR:
      case CHAR:
        return quoteString(value.toString());
      case INTEGER:
      case BIGINT:
      case SMALLINT:
      case TINYINT:
        return value.toString();
      case FLOAT:
      case DOUBLE:
      case REAL:
      case DECIMAL:
        return value.toString();
      case BOOLEAN:
        return ((Boolean) value) ? "true" : "false";
      case DATE:
      case TIME:
      case TIMESTAMP:
        return quoteString(value.toString());
      default:
        return quoteString(value.toString());
    }
  }

  @Override
  public String visitCall(RexCall call) {
    SqlOperator op = call.getOperator();

    if (op == SqlStdOperatorTable.AND) {
      String left = call.operands.get(0).accept(this);
      String right = call.operands.get(1).accept(this);
      return "(" + left + ") and (" + right + ")";
    } else if (op == SqlStdOperatorTable.OR) {
      String left = call.operands.get(0).accept(this);
      String right = call.operands.get(1).accept(this);
      return "(" + left + ") or (" + right + ")";
    } else if (op == SqlStdOperatorTable.NOT) {
      String operand = call.operands.get(0).accept(this);
      return "not(" + operand + ")";
    } else if (op == SqlStdOperatorTable.EQUALS) {
      String left = call.operands.get(0).accept(this);
      String right = call.operands.get(1).accept(this);
      return left + " == " + right;
    } else if (op == SqlStdOperatorTable.NOT_EQUALS) {
      String left = call.operands.get(0).accept(this);
      String right = call.operands.get(1).accept(this);
      return left + " != " + right;
    } else if (op == SqlStdOperatorTable.LESS_THAN) {
      String left = call.operands.get(0).accept(this);
      String right = call.operands.get(1).accept(this);
      return left + " < " + right;
    } else if (op == SqlStdOperatorTable.LESS_THAN_OR_EQUAL) {
      String left = call.operands.get(0).accept(this);
      String right = call.operands.get(1).accept(this);
      return left + " <= " + right;
    } else if (op == SqlStdOperatorTable.GREATER_THAN) {
      String left = call.operands.get(0).accept(this);
      String right = call.operands.get(1).accept(this);
      return left + " > " + right;
    } else if (op == SqlStdOperatorTable.GREATER_THAN_OR_EQUAL) {
      String left = call.operands.get(0).accept(this);
      String right = call.operands.get(1).accept(this);
      return left + " >= " + right;
    } else if (op == SqlStdOperatorTable.IS_NULL) {
      String operand = call.operands.get(0).accept(this);
      return "isnull(" + operand + ")";
    } else if (op == SqlStdOperatorTable.IS_NOT_NULL) {
      String operand = call.operands.get(0).accept(this);
      return "isnotnull(" + operand + ")";
    } else if (op == SqlStdOperatorTable.LIKE) {
      String left = call.operands.get(0).accept(this);
      String right = call.operands.get(1).accept(this);
      if (right.startsWith("'") && right.endsWith("%'")) {
        String prefix = right.substring(1, right.length() - 2);
        return left + " startswith " + quoteString(prefix);
      }
      return left + " contains " + right;
    } else {
      throw UserException.unsupportedError()
          .message("Unsupported operator in Sentinel filter: %s", op.getName())
          .build(logger);
    }
  }

  private static String quoteString(String str) {
    return "\"" + str.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
