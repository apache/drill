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
package org.apache.drill.exec.planner.sql.parser;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/** Parse tree for clearing the receiving Drillbit's physical plan cache. */
public final class SqlClearPlanCache extends SqlAlter {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CLEAR_PLAN_CACHE", SqlKind.OTHER_DDL) {
        @Override
        public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          return new SqlClearPlanCache(pos);
        }
      };

  public SqlClearPlanCache(SqlParserPos pos) {
    super(pos, "SYSTEM");
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Collections.emptyList();
  }

  @Override
  protected void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CLEAR PLAN CACHE");
  }
}
