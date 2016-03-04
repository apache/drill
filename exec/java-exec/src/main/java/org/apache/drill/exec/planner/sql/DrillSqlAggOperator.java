/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;

import java.util.ArrayList;
import java.util.List;

public class DrillSqlAggOperator extends SqlAggFunction {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlAggOperator.class);
  private final List<DrillFuncHolder> functions;

  public DrillSqlAggOperator(String name, List<DrillFuncHolder> functions, int argCount) {
    super(name,
        new SqlIdentifier(name, SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION,
        TypeInferenceUtils.getDrillSqlReturnTypeInference(
            name,
            functions),
        null,
        Checker.getChecker(argCount, argCount),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.functions = functions;
  }

  public List<DrillFuncHolder> getFunctions() {
    return functions;
  }
}
