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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlCallBinding;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlFunctionCategory;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlOperandCountRange;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.SqlOperandCountRanges;
import org.eigenbase.sql.type.SqlOperandTypeChecker;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;

public class HiveUDFOperator extends SqlFunction {

  public HiveUDFOperator(String name) {
    super(new SqlIdentifier(name, SqlParserPos.ZERO), DynamicReturnType.INSTANCE, null, new ArgChecker(), null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  @Override
  public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
    return validator.getTypeFactory().createSqlType(SqlTypeName.ANY);
  }

  /** Argument Checker for variable number of arguments */
  public static class ArgChecker implements SqlOperandTypeChecker {

    public static ArgChecker INSTANCE = new ArgChecker();

    private SqlOperandCountRange range = SqlOperandCountRanges.any();

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
      return range;
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
      return opName + "(HiveUDF - Opaque)";
    }
  }
}
