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

import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;

import java.util.Collection;
import java.util.List;

public class DrillSqlAggOperator extends SqlAggFunction {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlAggOperator.class);
  private final List<DrillFuncHolder> functions;

  protected DrillSqlAggOperator(String name, List<DrillFuncHolder> functions, int argCountMin, int argCountMax, SqlReturnTypeInference sqlReturnTypeInference) {
    super(name,
        new SqlIdentifier(name, SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION,
        sqlReturnTypeInference,
        null,
        Checker.getChecker(argCountMin, argCountMax),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.functions = functions;
  }

  private DrillSqlAggOperator(String name, List<DrillFuncHolder> functions, int argCountMin, int argCountMax) {
    this(name,
        functions,
        argCountMin,
        argCountMax,
        TypeInferenceUtils.getDrillSqlReturnTypeInference(
            name,
            functions));
  }

  public List<DrillFuncHolder> getFunctions() {
    return functions;
  }

  public static class DrillSqlAggOperatorBuilder {
    private String name = null;
    private final List<DrillFuncHolder> functions = Lists.newArrayList();
    private int argCountMin = Integer.MAX_VALUE;
    private int argCountMax = Integer.MIN_VALUE;

    public DrillSqlAggOperatorBuilder setName(final String name) {
      this.name = name;
      return this;
    }

    public DrillSqlAggOperatorBuilder addFunctions(Collection<DrillFuncHolder> functions) {
      this.functions.addAll(functions);
      return this;
    }

    public DrillSqlAggOperatorBuilder setArgumentCount(final int argCountMin, final int argCountMax) {
      this.argCountMin = Math.min(this.argCountMin, argCountMin);
      this.argCountMax = Math.max(this.argCountMax, argCountMax);
      return this;
    }

    public DrillSqlAggOperator build() {
      if(name == null || functions.isEmpty()) {
        throw new AssertionError("The fields, name and functions, need to be set before build DrillSqlAggOperator");
      }
      return new DrillSqlAggOperator(
          name,
          functions,
          argCountMin,
          argCountMax);
    }
  }
}
