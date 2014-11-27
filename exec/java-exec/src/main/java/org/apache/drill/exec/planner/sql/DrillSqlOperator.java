/**
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

import com.google.common.base.Preconditions;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlOperatorBinding;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;

public class DrillSqlOperator extends SqlFunction {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlOperator.class);

  private static final MajorType NONE = MajorType.getDefaultInstance();
  private final MajorType returnType;

  public DrillSqlOperator(String name, int argCount) {
    this(name, argCount, MajorType.getDefaultInstance());
  }

  public DrillSqlOperator(String name, int argCount, MajorType returnType) {
    super(new SqlIdentifier(name, SqlParserPos.ZERO), DynamicReturnType.INSTANCE, null, new Checker(argCount), null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.returnType = Preconditions.checkNotNull(returnType);
  }

  protected RelDataType getReturnDataType(final RelDataTypeFactory factory) {
    if (MinorType.BIT.equals(returnType.getMinorType())) {
      return factory.createSqlType(SqlTypeName.BOOLEAN);
    }
    return factory.createSqlType(SqlTypeName.ANY);
  }

  @Override
  public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
    if (NONE.equals(returnType)) {
      return validator.getTypeFactory().createSqlType(SqlTypeName.ANY);
    }
    return getReturnDataType(validator.getTypeFactory());
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    if (NONE.equals(returnType)) {
      return super.inferReturnType(opBinding);
    }
    return getReturnDataType(opBinding.getTypeFactory());
  }
}
