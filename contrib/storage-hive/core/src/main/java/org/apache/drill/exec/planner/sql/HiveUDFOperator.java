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

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

public class HiveUDFOperator extends SqlFunction {
  public HiveUDFOperator(String name, SqlReturnTypeInference sqlReturnTypeInference) {
    super(new SqlIdentifier(name, SqlParserPos.ZERO), sqlReturnTypeInference, null, ArgChecker.INSTANCE, null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  // Consider Hive functions to be non-deterministic so they are not folded at
  // planning time. The expression interpreter used to evaluate constant expressions
  // currently does not support anything but simple DrillFuncs.
  @Override
  public boolean isDeterministic() {
    return false;
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
    public Consistency getConsistency() {
      return Consistency.NONE;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
      return range;
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
      return opName + "(HiveUDF - Opaque)";
    }

    @Override
    public boolean isOptional(int arg) {
      return false;
    }
  }
}
