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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Rewrites special SQL function identifiers (like CURRENT_TIMESTAMP, SESSION_USER) to function calls
 * for Calcite 1.35+ compatibility.
 *
 * These are SQL standard functions that can be used without parentheses and are parsed as identifiers.
 * In Calcite 1.35+, they need to be converted to function calls before validation.
 */
public class SpecialFunctionRewriter extends SqlShuttle {

  // SQL special functions that can be used without parentheses and are parsed as identifiers
  private static final Set<String> SPECIAL_FUNCTIONS = new HashSet<>(Arrays.asList(
      "CURRENT_TIMESTAMP",
      "CURRENT_TIME",
      "CURRENT_DATE",
      "LOCALTIME",
      "LOCALTIMESTAMP",
      "CURRENT_USER",
      "SESSION_USER",
      "SYSTEM_USER",
      "USER",
      "CURRENT_PATH",
      "CURRENT_ROLE",
      "CURRENT_SCHEMA"
  ));

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (id.isSimple()) {
      String name = id.getSimple().toUpperCase();
      if (SPECIAL_FUNCTIONS.contains(name)) {
        SqlOperator operator = getOperatorFromName(name);
        if (operator != null) {
          // Create the function call
          SqlNode functionCall = operator.createCall(id.getParserPosition(), new SqlNode[0]);

          // Wrap with AS alias to preserve the original identifier name
          // This ensures SELECT session_user returns a column named "session_user" not "EXPR$0"
          SqlParserPos pos = id.getParserPosition();
          return SqlStdOperatorTable.AS.createCall(pos, functionCall, id);
        }
      }
    }
    return id;
  }

  private static SqlOperator getOperatorFromName(String name) {
    switch (name) {
      case "CURRENT_TIMESTAMP":
        return SqlStdOperatorTable.CURRENT_TIMESTAMP;
      case "CURRENT_TIME":
        return SqlStdOperatorTable.CURRENT_TIME;
      case "CURRENT_DATE":
        return SqlStdOperatorTable.CURRENT_DATE;
      case "LOCALTIME":
        return SqlStdOperatorTable.LOCALTIME;
      case "LOCALTIMESTAMP":
        return SqlStdOperatorTable.LOCALTIMESTAMP;
      case "CURRENT_USER":
        return SqlStdOperatorTable.CURRENT_USER;
      case "SESSION_USER":
        return SqlStdOperatorTable.SESSION_USER;
      case "SYSTEM_USER":
        return SqlStdOperatorTable.SYSTEM_USER;
      case "USER":
        return SqlStdOperatorTable.USER;
      case "CURRENT_PATH":
        return SqlStdOperatorTable.CURRENT_PATH;
      case "CURRENT_ROLE":
        return SqlStdOperatorTable.CURRENT_ROLE;
      case "CURRENT_SCHEMA":
        return SqlStdOperatorTable.CURRENT_SCHEMA;
      default:
        return null;
    }
  }
}
