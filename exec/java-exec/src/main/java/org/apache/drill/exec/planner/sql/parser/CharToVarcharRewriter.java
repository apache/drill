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

import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Rewrites CHAR literals to VARCHAR for Calcite 1.35+ compatibility.
 *
 * In Calcite 1.35+, single-character string literals are typed as CHAR(1) instead of VARCHAR.
 * This causes function signature mismatches for functions expecting VARCHAR.
 * This rewriter wraps CHAR literals with explicit CAST to VARCHAR.
 */
public class CharToVarcharRewriter extends SqlShuttle {

  @Override
  public SqlNode visit(SqlLiteral literal) {
    // Check if this is a CHAR literal
    if (literal.getTypeName() == SqlTypeName.CHAR) {
      // Create a VARCHAR data type spec without precision
      SqlBasicTypeNameSpec varcharTypeNameSpec = new SqlBasicTypeNameSpec(
          SqlTypeName.VARCHAR,
          literal.getParserPosition()
      );

      SqlDataTypeSpec varcharDataTypeSpec = new SqlDataTypeSpec(
          varcharTypeNameSpec,
          literal.getParserPosition()
      );

      // Wrap with CAST to VARCHAR
      return SqlStdOperatorTable.CAST.createCall(
          literal.getParserPosition(),
          literal,
          varcharDataTypeSpec
      );
    }
    return literal;
  }
}
