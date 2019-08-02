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

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;

/**
 * Argument Checker for variable number of arguments
 */
public class VarArgOperandTypeChecker implements SqlOperandTypeChecker {

  public static final VarArgOperandTypeChecker INSTANCE = new VarArgOperandTypeChecker();

  private static final SqlOperandCountRange ANY_RANGE = SqlOperandCountRanges.any();

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
    return ANY_RANGE;
  }

  @Override
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return opName + "(...)";
  }

  @Override
  public boolean isOptional(int arg) {
    return false;
  }
}
