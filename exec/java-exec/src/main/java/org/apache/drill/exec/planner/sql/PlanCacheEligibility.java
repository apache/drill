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

import java.util.Locale;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;

/** Checks whether a parsed query is safe to reuse without SQL validation. */
final class PlanCacheEligibility {
  private PlanCacheEligibility() { }

  static boolean isSafeToCache(SqlNode parsed, FunctionImplementationRegistry functions) {
    // SqlKind.OTHER_FUNCTION is a classification, not evidence of volatility.
    if (!parsed.getKind().belongsTo(SqlKind.QUERY)) {
      return false;
    }
    try {
      parsed.accept(new SqlShuttle() {
        @Override
        public SqlNode visit(SqlCall call) {
          if (!call.getOperator().isDeterministic()
              || call.getOperator().isDynamicFunction()
              || isQueryContextFunction(call)
              || (call.getKind() == SqlKind.OTHER_FUNCTION
                  && hasVolatileOverload(call, functions))) {
            throw UnsafeSql.INSTANCE;
          }
          return super.visit(call);
        }

        @Override
        public SqlNode visit(SqlDynamicParam param) {
          throw UnsafeSql.INSTANCE;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
          // Niladic functions can be parsed as identifiers rather than calls.
          if (id.isSimple() && isNiladicQueryContextName(id.getSimple())) {
            throw UnsafeSql.INSTANCE;
          }
          return super.visit(id);
        }
      });
      return true;
    } catch (UnsafeSql ignored) {
      return false;
    }
  }

  private static boolean isQueryContextFunction(SqlCall call) {
    String name = call.getOperator().getName().toUpperCase(Locale.ROOT);
    switch (name) {
      case "NOW":
      case "CURRENT_TIMESTAMP":
      case "LOCALTIMESTAMP":
      case "STATEMENT_TIMESTAMP":
      case "TRANSACTION_TIMESTAMP":
      case "CURRENT_DATE":
      case "CURRENT_TIME":
      case "LOCALTIME":
      case "SESSION_ID":
      case "CURRENT_SCHEMA":
      case "USER":
      case "SESSION_USER":
      case "SYSTEM_USER":
        return true;
      case "UNIX_TIMESTAMP":
        return call.getOperandList().isEmpty();
      default:
        return false;
    }
  }

  private static boolean isNiladicQueryContextName(String name) {
    switch (name.toUpperCase(Locale.ROOT)) {
      case "CURRENT_TIMESTAMP":
      case "LOCALTIMESTAMP":
      case "CURRENT_DATE":
      case "CURRENT_TIME":
      case "LOCALTIME":
      case "SESSION_ID":
      case "CURRENT_SCHEMA":
      case "USER":
      case "SESSION_USER":
      case "SYSTEM_USER":
        return true;
      default:
        return false;
    }
  }

  private static boolean hasVolatileOverload(SqlCall call,
      FunctionImplementationRegistry functions) {
    // Before validation, Drill functions may still be unresolved Calcite
    // functions whose determinism flag is always true.
    for (DrillFuncHolder holder : functions.getLocalFunctionRegistry()
        .getMethods(call.getOperator().getName())) {
      if (!holder.isDeterministic()) {
        return true;
      }
    }
    return false;
  }

  private static final class UnsafeSql extends RuntimeException {
    private static final UnsafeSql INSTANCE = new UnsafeSql();

    private UnsafeSql() {
      super(null, null, false, false);
    }
  }
}
