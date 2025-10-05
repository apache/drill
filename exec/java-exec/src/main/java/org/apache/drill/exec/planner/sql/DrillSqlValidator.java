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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * Custom SqlValidator for Drill that extends Calcite's SqlValidatorImpl.
 *
 * This validator provides Drill-specific validation behavior, particularly
 * for handling star identifiers (*) in aggregate function contexts.
 *
 * Note: Special SQL functions like CURRENT_TIMESTAMP, SESSION_USER, etc. are
 * rewritten to function calls before validation in SqlConverter.validate().
 */
public class DrillSqlValidator extends SqlValidatorImpl {

  public DrillSqlValidator(
      SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory,
      Config config) {
    super(opTab, catalogReader, typeFactory, config);
  }

  @Override
  public RelDataType deriveType(SqlValidatorScope scope, SqlNode operand) {
    // For Calcite 1.35+ compatibility: Handle star identifiers in aggregate functions
    // The star identifier should return a special marker type rather than trying
    // to resolve it as a column reference
    if (operand instanceof SqlIdentifier) {
      SqlIdentifier identifier = (SqlIdentifier) operand;
      if (identifier.isStar()) {
        // For star identifiers, return a simple BIGINT type as a placeholder
        // The actual type will be determined during conversion to relational algebra
        // This prevents "Unknown identifier '*'" errors during validation
        return typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
      }
    }

    // For Calcite 1.35+ compatibility: Try to derive type, and if it fails due to
    // function signature mismatch, it might be because CHARACTER literals need
    // to be coerced to VARCHAR
    try {
      return super.deriveType(scope, operand);
    } catch (org.apache.calcite.runtime.CalciteContextException e) {
      // Check if this is a function signature mismatch error
      if (e.getCause() instanceof org.apache.calcite.sql.validate.SqlValidatorException) {
        String message = e.getMessage();
        // If the error mentions CHARACTER type in function signature, retry with type coercion
        if (message != null && message.contains("CHARACTER") && message.contains("No match found")) {
          // Let Calcite handle this through implicit casting/coercion
          // by enabling type coercion in the config (already done in SqlConverter)
          // Just rethrow for now - the real fix is in the type coercion system
        }
      }
      throw e;
    }
  }
}
