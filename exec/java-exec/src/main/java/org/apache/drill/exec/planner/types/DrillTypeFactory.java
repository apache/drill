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
package org.apache.drill.exec.planner.types;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.exceptions.UserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drill's type factory that wraps Calcite's JavaTypeFactoryImpl and validates
 * DECIMAL types to ensure they have valid precision and scale specifications.
 *
 * This factory enforces Drill's DECIMAL constraints:
 * - Precision must be >= 1
 * - Scale must be <= precision
 * - Maximum precision is 38
 *
 * Invalid specifications are rejected with validation errors as expected by
 * Drill's SQL semantics and test suite.
 */
public class DrillTypeFactory extends JavaTypeFactoryImpl {

  private static final Logger logger = LoggerFactory.getLogger(DrillTypeFactory.class);
  private static final int DRILL_MAX_NUMERIC_PRECISION = 38;

  public DrillTypeFactory(RelDataTypeSystem typeSystem) {
    super(typeSystem);
  }

  /**
   * Override createSqlType.
   */
  @Override
  public RelDataType createSqlType(SqlTypeName typeName) {
    return super.createSqlType(typeName);
  }

  /**
   * Override createSqlType.
   */
  @Override
  public RelDataType createSqlType(SqlTypeName typeName, int precision) {
    return super.createSqlType(typeName, precision);
  }

  /**
   * Override createSqlType to validate DECIMAL precision and scale.
   * This is the primary entry point for DECIMAL type creation with both precision and scale.
   *
   * Calcite 1.38 allows creation of invalid DECIMAL types in some intermediate operations,
   * but we need to reject obviously invalid user-specified types.
   */
  @Override
  public RelDataType createSqlType(SqlTypeName typeName, int precision, int scale) {
    // Validate DECIMAL precision and scale
    if (typeName == SqlTypeName.DECIMAL) {
      // Reject precision < 1
      if (precision < 1) {
        throw UserException.validationError()
            .message("Expected precision greater than 0, but was %s.", precision)
            .build(logger);
      }

      // Reject scale > precision
      if (scale > precision) {
        throw UserException.validationError()
            .message("Expected scale less than or equal to precision, " +
                "but was precision %s and scale %s.", precision, scale)
            .build(logger);
      }

      // Cap at Drill's maximum precision
      if (precision > DRILL_MAX_NUMERIC_PRECISION) {
        logger.warn("DECIMAL precision {} exceeds Drill maximum {}, capping to maximum",
            precision, DRILL_MAX_NUMERIC_PRECISION);
        precision = DRILL_MAX_NUMERIC_PRECISION;
        // Also cap scale if needed
        if (scale > precision) {
          scale = precision;
        }
      }
    }

    return super.createSqlType(typeName, precision, scale);
  }

  /**
   * Override createTypeWithNullability to pass through without modifications.
   * Validation happens in createSqlType().
   */
  @Override
  public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
    return super.createTypeWithNullability(type, nullable);
  }
}
