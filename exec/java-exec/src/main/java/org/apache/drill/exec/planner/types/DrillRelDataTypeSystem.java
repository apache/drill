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
        return 38;
      default:
        return super.getDefaultPrecision(typeName);
    }
  }

  @Override
  public int getMaxScale(SqlTypeName typeName) {
    switch (typeName) {
      case DECIMAL:
        return 38;
      default:
        return super.getMaxScale(typeName);
    }
  }

  @Override
  public int getMaxPrecision(SqlTypeName typeName) {
    switch (typeName) {
      case DECIMAL:
        return 38;
      default:
        return super.getMaxPrecision(typeName);
    }
  }

  @Override
  public boolean isSchemaCaseSensitive() {
    // Drill uses case-insensitive policy
    return false;
  }

  @Override
  public RelDataType deriveDecimalPlusType(RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    // Override to maintain Drill's decimal precision behavior for addition operations
    // This helps ensure consistent decimal type inference in union operations
    RelDataType result = super.deriveDecimalPlusType(typeFactory, type1, type2);
    if (result != null && result.getSqlTypeName() == SqlTypeName.DECIMAL) {
      // Ensure we don't exceed Drill's maximum decimal precision/scale
      int precision = Math.min(result.getPrecision(), getMaxPrecision(SqlTypeName.DECIMAL));
      int scale = Math.min(result.getScale(), getMaxScale(SqlTypeName.DECIMAL));
      if (precision != result.getPrecision() || scale != result.getScale()) {
        result = typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
      }
    }
    return result;
  }

  @Override
  public RelDataType deriveDecimalDivideType(RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    // Override to maintain Drill's decimal precision behavior for division operations
    RelDataType result = super.deriveDecimalDivideType(typeFactory, type1, type2);
    if (result != null && result.getSqlTypeName() == SqlTypeName.DECIMAL) {
      // Ensure we don't exceed Drill's maximum decimal precision/scale
      int precision = Math.min(result.getPrecision(), getMaxPrecision(SqlTypeName.DECIMAL));
      int scale = Math.min(result.getScale(), getMaxScale(SqlTypeName.DECIMAL));
      if (precision != result.getPrecision() || scale != result.getScale()) {
        result = typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
      }
    }
    return result;
  }

  @Override
  public RelDataType deriveDecimalMultiplyType(RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    // Override to maintain Drill's decimal precision behavior for multiplication operations
    RelDataType result = super.deriveDecimalMultiplyType(typeFactory, type1, type2);
    if (result != null && result.getSqlTypeName() == SqlTypeName.DECIMAL) {
      // Ensure we don't exceed Drill's maximum decimal precision/scale
      int precision = Math.min(result.getPrecision(), getMaxPrecision(SqlTypeName.DECIMAL));
      int scale = Math.min(result.getScale(), getMaxScale(SqlTypeName.DECIMAL));
      if (precision != result.getPrecision() || scale != result.getScale()) {
        result = typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
      }
    }
    return result;
  }

}
