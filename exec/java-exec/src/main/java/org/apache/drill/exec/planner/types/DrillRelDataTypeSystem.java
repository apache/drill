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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.types.Types;

public class DrillRelDataTypeSystem extends RelDataTypeSystemImpl {

  public static final RelDataTypeSystem DRILL_REL_DATATYPE_SYSTEM = new DrillRelDataTypeSystem();

  @Override
  public int getDefaultPrecision(SqlTypeName typeName) {
    switch (typeName) {
      case CHAR:
      case BINARY:
      case VARCHAR:
      case VARBINARY:
        return Types.MAX_VARCHAR_LENGTH;
      case TIMESTAMP:
      case TIME:
        return Types.DEFAULT_TIMESTAMP_PRECISION;
      case DECIMAL:
        // Calcite 1.38 changed default from 38 to 19, but Drill uses 38
        return 38;
      default:
        return super.getDefaultPrecision(typeName);
    }
  }

  @Override
  public int getMaxScale(SqlTypeName typeName) {
    if (typeName == SqlTypeName.DECIMAL) {
      return 38;
    }
    return super.getMaxScale(typeName);
  }

  @Override
  public int getMaxPrecision(SqlTypeName typeName) {
    if (typeName == SqlTypeName.DECIMAL) {
      return 38;
    }
    return super.getMaxPrecision(typeName);
  }

  @Override
  @Deprecated
  public int getMaxNumericPrecision() {
    // Override deprecated method for compatibility with Calcite internals that still call it
    // Calcite 1.38 changed this from 38 to 19, but Drill needs 38
    return 38;
  }

  @Override
  @Deprecated
  public int getMaxNumericScale() {
    // Override deprecated method for compatibility with Calcite internals that still call it
    // Drill needs max scale of 38 for DECIMAL
    return 38;
  }

  @Override
  public boolean isSchemaCaseSensitive() {
    // Drill uses case-insensitive policy
    return false;
  }

  @Override
  public RelDataType deriveDecimalMultiplyType(RelDataTypeFactory typeFactory,
                                                RelDataType type1,
                                                RelDataType type2) {
    // For Calcite 1.38 compatibility: Compute our own type instead of calling super
    // Calcite's super implementation uses its own getMaxPrecision() which returns 19

    if (type1.getSqlTypeName() != SqlTypeName.DECIMAL || type2.getSqlTypeName() != SqlTypeName.DECIMAL) {
      return null; // Not a DECIMAL operation
    }

    int p1 = type1.getPrecision();
    int s1 = type1.getScale();
    int p2 = type2.getPrecision();
    int s2 = type2.getScale();

    // SQL:2003 standard formula for multiplication
    int precision = p1 + p2;
    int scale = s1 + s2;

    // Drill's max precision is 38
    int maxPrecision = 38;

    // Cap precision at maximum
    if (precision > maxPrecision) {
      precision = maxPrecision;
    }

    // Ensure scale doesn't exceed precision
    if (scale > precision) {
      scale = precision;
    }

    return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
  }

  @Override
  public RelDataType deriveDecimalDivideType(RelDataTypeFactory typeFactory,
                                              RelDataType type1,
                                              RelDataType type2) {
    // For Calcite 1.38 compatibility: Compute our own type instead of calling super
    // Calcite's super implementation uses its own getMaxPrecision() which returns 19

    if (type1.getSqlTypeName() != SqlTypeName.DECIMAL || type2.getSqlTypeName() != SqlTypeName.DECIMAL) {
      return null; // Not a DECIMAL operation
    }

    int p1 = type1.getPrecision();
    int s1 = type1.getScale();
    int p2 = type2.getPrecision();
    int s2 = type2.getScale();

    // SQL:2003 standard formula for division
    int integerDigits = p1 - s1 + s2;  // Whole digits
    int scale = Math.max(6, s1 + p2 + 1);  // Scale (minimum 6)
    int precision = integerDigits + scale;

    // Drill's max precision is 38
    int maxPrecision = 38;

    // If precision exceeds max, reduce scale while preserving integer digits
    if (precision > maxPrecision) {
      if (integerDigits >= maxPrecision) {
        precision = maxPrecision;
        scale = 0;
      } else {
        precision = maxPrecision;
        scale = maxPrecision - integerDigits;
      }
    }

    // Ensure scale doesn't exceed precision
    if (scale > precision) {
      scale = precision;
    }

    return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
  }

  @Override
  public RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
    // For Calcite 1.38 compatibility: ensure SUM result has valid precision/scale
    // SUM should have same scale as input, but increased precision to avoid overflow
    RelDataType sumType = super.deriveSumType(typeFactory, argumentType);

    if (sumType.getSqlTypeName() == SqlTypeName.DECIMAL) {
      int precision = sumType.getPrecision();
      int scale = sumType.getScale();

      // Ensure scale doesn't exceed precision (Calcite 1.38 bug)
      if (scale > precision) {
        scale = precision;
      }

      // Ensure we have Drill's max precision if needed
      if (precision < 38) {
        precision = 38;
      }

      return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
    }

    return sumType;
  }

  @Override
  public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
    // For Calcite 1.38 compatibility: ensure AVG result has valid precision/scale
    // AVG increases scale to provide fractional results
    RelDataType avgType = super.deriveAvgAggType(typeFactory, argumentType);

    if (avgType.getSqlTypeName() == SqlTypeName.DECIMAL) {
      int precision = avgType.getPrecision();
      int scale = avgType.getScale();

      // Ensure scale doesn't exceed precision (Calcite 1.38 bug)
      if (scale > precision) {
        scale = precision;
      }

      // Ensure we have Drill's max precision
      if (precision < 38) {
        precision = 38;
      }

      return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
    }

    return avgType;
  }

  @Override
  public RelDataType deriveCovarType(RelDataTypeFactory typeFactory, RelDataType arg0Type, RelDataType arg1Type) {
    // For Calcite 1.38 compatibility: ensure COVAR/STDDEV/VAR result has valid precision/scale
    RelDataType covarType = super.deriveCovarType(typeFactory, arg0Type, arg1Type);

    if (covarType.getSqlTypeName() == SqlTypeName.DECIMAL) {
      int precision = covarType.getPrecision();
      int scale = covarType.getScale();

      // Drill's max precision is 38
      int maxPrecision = 38;

      // First, cap precision at Drill's maximum
      if (precision > maxPrecision) {
        precision = maxPrecision;
      }

      // Then ensure scale doesn't exceed the (possibly capped) precision
      if (scale > precision) {
        scale = precision;
      }

      return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
    }

    return covarType;
  }

  @Override
  public RelDataType deriveDecimalPlusType(RelDataTypeFactory typeFactory,
                                            RelDataType type1,
                                            RelDataType type2) {
    // For Calcite 1.38 compatibility: Compute our own type instead of calling super
    // Calcite's super implementation uses its own getMaxPrecision() which returns 19
    // We need to use Drill's max precision of 38

    if (type1.getSqlTypeName() != SqlTypeName.DECIMAL || type2.getSqlTypeName() != SqlTypeName.DECIMAL) {
      return null; // Not a DECIMAL operation
    }

    int p1 = type1.getPrecision();
    int s1 = type1.getScale();
    int p2 = type2.getPrecision();
    int s2 = type2.getScale();

    // Result scale is max of the two scales
    int scale = Math.max(s1, s2);

    // Calculate integer digits needed (before decimal point)
    int integerDigits = Math.max(p1 - s1, p2 - s2) + 1; // +1 for potential carry

    // Result precision
    int precision = integerDigits + scale;

    // Drill's max precision is 38
    int maxPrecision = 38;

    // If precision exceeds max, we need to reduce scale while preserving integer digits
    if (precision > maxPrecision) {
      // Ensure integer digits fit, reduce scale if necessary
      if (integerDigits >= maxPrecision) {
        // All available precision goes to integer part
        precision = maxPrecision;
        scale = 0;
      } else {
        // We have room for some scale
        precision = maxPrecision;
        scale = maxPrecision - integerDigits;
      }
    }

    return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
  }

}
