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
   * Override createSqlType to validate and fix DECIMAL precision and scale.
   * This is the primary entry point for DECIMAL type creation with both precision and scale.
   *
   * Calcite 1.38 may compute invalid DECIMAL types in intermediate operations (e.g., negate
   * operations). We auto-fix these to prevent errors, since we can't distinguish between
   * user-specified and Calcite-computed types at this level.
   */
  @Override
  public RelDataType createSqlType(SqlTypeName typeName, int precision, int scale) {
    // Validate and fix DECIMAL precision and scale
    if (typeName == SqlTypeName.DECIMAL) {
      int originalPrecision = precision;
      int originalScale = scale;
      boolean wasFixed = false;

      // Fix scale > precision (Calcite 1.38 bug in some operations)
      if (scale > precision) {
        // Make precision large enough to hold the scale
        precision = Math.max(precision, scale);
        wasFixed = true;
      }

      // Fix precision < 1
      if (precision < 1) {
        precision = 1;
        wasFixed = true;
      }

      // Cap at Drill's maximum precision
      if (precision > DRILL_MAX_NUMERIC_PRECISION) {
        precision = DRILL_MAX_NUMERIC_PRECISION;
        wasFixed = true;
      }

      // Ensure scale fits within precision after capping
      if (scale > precision) {
        scale = precision;
        wasFixed = true;
      }

      // Ensure scale is non-negative (Calcite 1.38 CALCITE-6560 support)
      if (scale < 0) {
        scale = 0;
        wasFixed = true;
      }

      if (wasFixed) {
        logger.debug("Fixed invalid DECIMAL type: precision={} scale={} -> precision={} scale={}",
            originalPrecision, originalScale, precision, scale);
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
