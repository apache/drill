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
  public boolean isSchemaCaseSensitive() {
    // Drill uses case-insensitive policy
    return false;
  }

  @Override
  public RelDataType deriveDecimalMultiplyType(RelDataTypeFactory typeFactory,
                                                RelDataType type1,
                                                RelDataType type2) {
    // For Calcite 1.38 compatibility: ensure multiplication result has valid precision/scale
    RelDataType multiplyType = super.deriveDecimalMultiplyType(typeFactory, type1, type2);

    if (multiplyType == null) {
      return null; // Not a DECIMAL multiplication (e.g., ANY * ANY)
    }

    if (multiplyType.getSqlTypeName() == SqlTypeName.DECIMAL) {
      int precision = multiplyType.getPrecision();
      int scale = multiplyType.getScale();

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

    return multiplyType;
  }

  @Override
  public RelDataType deriveDecimalDivideType(RelDataTypeFactory typeFactory,
                                              RelDataType type1,
                                              RelDataType type2) {
    // For Calcite 1.38 compatibility: ensure division result has valid precision/scale
    RelDataType divideType = super.deriveDecimalDivideType(typeFactory, type1, type2);

    if (divideType == null) {
      return null; // Not a DECIMAL division (e.g., ANY / ANY)
    }

    if (divideType.getSqlTypeName() == SqlTypeName.DECIMAL) {
      int precision = divideType.getPrecision();
      int scale = divideType.getScale();

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

    return divideType;
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

}
