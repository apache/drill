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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;

public class DrillSqlOperator extends SqlFunction {
  // static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlOperator.class);
  private final boolean isDeterministic;
  private final List<DrillFuncHolder> functions;

  /**
   * This constructor exists for the legacy reason.
   *
   * It is because Drill cannot access to DrillOperatorTable at the place where this constructor is being called.
   * In principle, if Drill needs a DrillSqlOperator, it is supposed to go to DrillOperatorTable for pickup.
   */
  @Deprecated
  public DrillSqlOperator(String name, int argCount, boolean isDeterministic) {
    this(name, new ArrayList<DrillFuncHolder>(), argCount, argCount, isDeterministic);
  }

  public DrillSqlOperator(String name, List<DrillFuncHolder> functions, int argCountMin, int argCountMax, boolean isDeterministic) {
    super(new SqlIdentifier(name, SqlParserPos.ZERO),
        TypeInferenceUtils.getDrillSqlReturnTypeInference(
            name,
            functions),
        null,
        Checker.getChecker(argCountMin, argCountMax),
        null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.functions = functions;
    this.isDeterministic = isDeterministic;
  }

  @Override
  public boolean isDeterministic() {
    return isDeterministic;
  }

  public List<DrillFuncHolder> getFunctions() {
    return functions;
  }
}