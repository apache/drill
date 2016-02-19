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

import com.google.common.base.Preconditions;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.drill.exec.planner.logical.DrillConstExecutor;

public class DrillSqlOperator extends SqlFunction {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlOperator.class);

  private static final MajorType NONE = MajorType.getDefaultInstance();
  private final MajorType returnType;
  private final boolean isDeterministic;

  public DrillSqlOperator(String name, int argCount, boolean isDeterministic) {
    this(name, argCount, MajorType.getDefaultInstance(), isDeterministic);
  }

  public DrillSqlOperator(String name, int argCount, MajorType returnType, boolean isDeterminisitic) {
    super(new SqlIdentifier(name, SqlParserPos.ZERO), DynamicReturnType.INSTANCE, null, new Checker(argCount), null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.returnType = Preconditions.checkNotNull(returnType);
    this.isDeterministic = isDeterminisitic;
  }

  public boolean isDeterministic() {
    return isDeterministic;
  }

  private RelDataType getReturnType(final RelDataTypeFactory factory) {
    // least restrictive type (nullable ANY type)
    final RelDataType anyType = factory.createSqlType(SqlTypeName.ANY);
    final RelDataType nullableAnyType = factory.createTypeWithNullability(anyType, true);

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
  public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
    /*
     * We return a nullable output type both in validation phase and in
     * Sql to Rel phase. We don't know the type of the output until runtime
     * hence have to choose the least restrictive type to avoid any wrong
     * results.
     */
    return getReturnType(validator.getTypeFactory());
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    return getReturnType(opBinding.getTypeFactory());
  }
}
