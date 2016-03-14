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
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
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
  public DrillSqlOperator(final String name, final int argCount, final boolean isDeterministic) {
    this(name,
        argCount,
        isDeterministic,
        DynamicReturnType.INSTANCE);
  }

  /**
   * This constructor exists for the legacy reason.
   *
   * It is because Drill cannot access to DrillOperatorTable at the place where this constructor is being called.
   * In principle, if Drill needs a DrillSqlOperator, it is supposed to go to DrillOperatorTable for pickup.
   */
  @Deprecated
  public DrillSqlOperator(final String name, final int argCount, final boolean isDeterministic,
      final SqlReturnTypeInference sqlReturnTypeInference) {
    this(name,
        new ArrayList<DrillFuncHolder>(),
        argCount,
        argCount,
        isDeterministic,
        sqlReturnTypeInference);
  }

  /**
   * This constructor exists for the legacy reason.
   *
   * It is because Drill cannot access to DrillOperatorTable at the place where this constructor is being called.
   * In principle, if Drill needs a DrillSqlOperator, it is supposed to go to DrillOperatorTable for pickup.
   */
  @Deprecated
  public DrillSqlOperator(final String name, final int argCount, final boolean isDeterministic, final RelDataType type) {
    this(name,
        new ArrayList<DrillFuncHolder>(),
        argCount,
        argCount,
        isDeterministic, new SqlReturnTypeInference() {
          @Override
          public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            return type;
          }
        });
  }

  protected DrillSqlOperator(String name, List<DrillFuncHolder> functions, int argCountMin, int argCountMax, boolean isDeterministic,
      SqlReturnTypeInference sqlReturnTypeInference) {
    super(new SqlIdentifier(name, SqlParserPos.ZERO),
        sqlReturnTypeInference,
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

  public static class DrillSqlOperatorBuilder {
    private String name;
    private final List<DrillFuncHolder> functions = Lists.newArrayList();
    private int argCountMin = Integer.MAX_VALUE;
    private int argCountMax = Integer.MIN_VALUE;
    private boolean isDeterministic = true;

    public DrillSqlOperatorBuilder setName(final String name) {
      this.name = name;
      return this;
    }

    public DrillSqlOperatorBuilder addFunctions(Collection<DrillFuncHolder> functions) {
      this.functions.addAll(functions);
      return this;
    }

    public DrillSqlOperatorBuilder setArgumentCount(final int argCountMin, final int argCountMax) {
      this.argCountMin = Math.min(this.argCountMin, argCountMin);
      this.argCountMax = Math.max(this.argCountMax, argCountMax);
      return this;
    }

    public DrillSqlOperatorBuilder setDeterministic(boolean isDeterministic) {
      /* By the logic here, we will group the entire Collection as a DrillSqlOperator. and claim it is non-deterministic.
       * Add if there is a non-deterministic DrillFuncHolder, then we claim this DrillSqlOperator is non-deterministic.
       *
       * In fact, in this case, separating all DrillFuncHolder into two DrillSqlOperator
       * (one being deterministic and the other being non-deterministic does not help) since in DrillOperatorTable.lookupOperatorOverloads(),
       * parameter list is not passed in. So even if we have two DrillSqlOperator, DrillOperatorTable.lookupOperatorOverloads()
       * does not have enough information to pick the one matching the argument list.
       */
      if(this.isDeterministic) {
        this.isDeterministic = isDeterministic;
      }
      return this;
    }

    public DrillSqlOperator build() {
      if(name == null || functions.isEmpty()) {
        throw new AssertionError("The fields, name and functions, need to be set before build DrillSqlAggOperator");
      }

      return new DrillSqlOperator(
          name,
          functions,
          argCountMin,
          argCountMax,
          isDeterministic,
          TypeInferenceUtils.getDrillSqlReturnTypeInference(
              name,
              functions));
    }
  }
}