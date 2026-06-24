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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
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
      "CURRENT_SCHEMA",
      "SESSION_ID"  // Drill-specific niladic function
  ));

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (id.isSimple()) {
      String name = id.getSimple().toUpperCase();
      if (SPECIAL_FUNCTIONS.contains(name)) {
        // For Calcite 1.35+ compatibility: Create unresolved function calls for all niladic functions
        // This allows Drill's operator table lookup to find Drill UDFs that may shadow Calcite built-ins
        // (like user, session_user, system_user, current_schema)
        SqlParserPos pos = id.getParserPosition();
        SqlIdentifier functionId = new SqlIdentifier(name, pos);
        SqlNode functionCall = new SqlBasicCall(
            new org.apache.calcite.sql.SqlUnresolvedFunction(
                functionId,
                null,
                null,
                null,
                null,
                org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
            new SqlNode[0],
            pos);
        // Wrap with AS alias to preserve the original identifier name
        // This ensures SELECT session_user returns a column named "session_user" not "EXPR$0"
        return SqlStdOperatorTable.AS.createCall(pos, functionCall, id);
      }
    }
    return id;
  }
}
