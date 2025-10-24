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
 * Drill's type factory that wraps Calcite's JavaTypeFactoryImpl and fixes
 * invalid DECIMAL types created by Calcite 1.38's CALCITE-6427 regression.
 *
 * This factory ensures that all DECIMAL types have valid precision and scale
 * where scale <= precision and precision >= 1, preventing IllegalArgumentException
 * in RexLiteral constructor.
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
   * Override createSqlType to fix invalid DECIMAL types before they're created.
   * This is the primary entry point for DECIMAL type creation with both precision and scale.
   */
  @Override
  public RelDataType createSqlType(SqlTypeName typeName, int precision, int scale) {
    // System.out.println("DrillTypeFactory.createSqlType(typeName=" + typeName + ", precision=" + precision + ", scale=" + scale + ")");

    // Fix invalid DECIMAL types before creating them
    if (typeName == SqlTypeName.DECIMAL) {
      // Validate and fix precision/scale
      if (scale > precision || precision < 1) {
        int originalPrecision = precision;
        int originalScale = scale;

        // Ensure precision is at least as large as scale
        precision = Math.max(precision, scale);

        // Cap precision at Drill's maximum
        precision = Math.min(precision, DRILL_MAX_NUMERIC_PRECISION);

        // Ensure scale doesn't exceed the corrected precision
        scale = Math.min(scale, precision);

        // Ensure precision is at least 1
        precision = Math.max(precision, 1);

        // System.out.println("DrillTypeFactory: FIXED invalid DECIMAL type: " +
        //     "precision=" + originalPrecision + " scale=" + originalScale +
        //     " -> precision=" + precision + " scale=" + scale);

        logger.warn("DrillTypeFactory: Fixed invalid DECIMAL type: " +
            "precision={} scale={} -> precision={} scale={}",
            originalPrecision, originalScale, precision, scale);
      }
    }

    return super.createSqlType(typeName, precision, scale);
  }

  /**
   * Override createTypeWithNullability to intercept all type creation.
   */
  @Override
  public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
    // Check if the type being wrapped is an invalid DECIMAL
    if (type.getSqlTypeName() == SqlTypeName.DECIMAL) {
      int precision = type.getPrecision();
      int scale = type.getScale();
      if (scale > precision || precision < 1) {
        // System.out.println("DrillTypeFactory.createTypeWithNullability: Found invalid DECIMAL type: " +
        //     "precision=" + precision + " scale=" + scale + ", recreating with fix");
        // Recreate the type with fixed precision/scale
        type = createSqlType(SqlTypeName.DECIMAL, precision, scale);
      }
    }
    return super.createTypeWithNullability(type, nullable);
  }
}
