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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.planner.logical.DrillConstExecutor;
import org.apache.drill.exec.resolver.TypeCastRules;
import org.apache.drill.exec.util.AssertionUtil;

import java.util.LinkedList;
import java.util.List;

public class DrillSqlOperator extends SqlFunction {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlOperator.class);

  private static final MajorType NONE = MajorType.getDefaultInstance();
  private final boolean isDeterministic;
  private final List<DrillFuncHolder> functions;

  public DrillSqlOperator(String name, int argCount, boolean isDeterministic) {
    this(name, null, argCount, isDeterministic);
  }

  public DrillSqlOperator(String name, int argCount, MajorType returnType, boolean isDeterministic) {
    super(new SqlIdentifier(name, SqlParserPos.ZERO), null, null, new Checker(argCount), null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.isDeterministic = isDeterministic;
    this.functions = Lists.newArrayList();
  }

  public DrillSqlOperator(String name, List<DrillFuncHolder> functions, int argCount, boolean isDeterministic) {
    super(new SqlIdentifier(name, SqlParserPos.ZERO), null, null, new Checker(argCount), null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.functions = functions;
    this.isDeterministic = isDeterministic;
  }

  @Override
  public boolean isDeterministic() {
    return isDeterministic;
  }

  private static List<MajorType> getMajorTypes(List<RelDataType> relTypes) {
    List<MajorType> drillTypes = Lists.newArrayList();
    for (RelDataType relType : relTypes) {
      drillTypes.add(getMajorType(relType));
    }
    return drillTypes;
  }

  private static MajorType getMajorType(RelDataType relDataType) {
    final MinorType minorType = DrillConstExecutor.CALCITE_TO_DRILL_MAPPING.get(relDataType.getSqlTypeName());
    if (relDataType.isNullable()) {
      return Types.optional(minorType);
    } else {
      return Types.required(minorType);
    }
  }

  /**
   * Same as {@link org.apache.drill.exec.resolver.DefaultFunctionResolver#getBestMatch(List, FunctionCall)} for
   * correctness.
   */
  private DrillFuncHolder getFunction(SqlOperatorBinding opBinding) {
    int bestcost = Integer.MAX_VALUE;
    int currcost = Integer.MAX_VALUE;
    DrillFuncHolder bestmatch = null;
    final List<DrillFuncHolder> bestMatchAlternatives = new LinkedList<>();

    for (DrillFuncHolder h : functions) {

      currcost = TypeCastRules.getCost(getMajorTypes(opBinding.collectOperandTypes()), h);

      // if cost is lower than 0, func implementation is not matched, either w/ or w/o implicit casts
      if (currcost < 0) {
        continue;
      }

      if (currcost < bestcost) {
        bestcost = currcost;
        bestmatch = h;
        bestMatchAlternatives.clear();
      } else if (currcost == bestcost) {
        // keep log of different function implementations that have the same best cost
        bestMatchAlternatives.add(h);
      }
    }

    if (bestcost < 0) {
      //did not find a matched func implementation, either w/ or w/o implicit casts
      //TODO: raise exception here?
      return null;
    } else {
      if (AssertionUtil.isAssertionsEnabled() && bestMatchAlternatives.size() > 0) {
        /*
         * There are other alternatives to the best match function which could have been selected
         * Log the possible functions and the chose implementation and raise an exception
         */
        logger.error("Chosen function impl: " + bestmatch);

        // printing the possible matches
        logger.error("Printing all the possible functions that could have matched: ");
        for (DrillFuncHolder holder : bestMatchAlternatives) {
          logger.error(holder.toString());
        }

        throw new AssertionError("Multiple functions with best cost found");
      }
      return bestmatch;
    }
  }

  private RelDataType getReturnType(final RelDataTypeFactory factory, DrillFuncHolder func) {
    // least restrictive type (nullable ANY type)
    final RelDataType anyType = factory.createSqlType(SqlTypeName.ANY);
    final RelDataType nullableAnyType = factory.createTypeWithNullability(anyType, true);

    final MajorType returnType = func.getReturnType();
    if (NONE.equals(returnType)) {
      return nullableAnyType;
    }

    final MinorType minorType = returnType.getMinorType();
    final SqlTypeName sqlTypeName = DrillConstExecutor.DRILL_TO_CALCITE_TYPE_MAPPING.get(minorType);
    if (sqlTypeName == null) {
      return factory.createTypeWithNullability(nullableAnyType, true);
    }

    final RelDataType relReturnType = factory.createSqlType(sqlTypeName);
    switch (returnType.getMode()) {
    case OPTIONAL:
      return factory.createTypeWithNullability(relReturnType, true);
    case REQUIRED:
      return relReturnType;
    case REPEATED:
      return relReturnType;
    default:
      return nullableAnyType;
    }
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    System.out.println("OP BINDING OPERATOR: " + opBinding.getOperator());
    System.out.println("OP BINDING: " + opBinding.collectOperandTypes());
    for (RelDataType type : opBinding.collectOperandTypes()) {
      if (type.getSqlTypeName() == SqlTypeName.ANY) {
        return opBinding.getTypeFactory()
            .createTypeWithNullability(opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY), true);
      }
    }
    DrillFuncHolder func = getFunction(opBinding);
    return getReturnType(opBinding.getTypeFactory(), func);
  }

  @Override
  public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
    return super.deriveType(validator, scope, call);
  }
}
