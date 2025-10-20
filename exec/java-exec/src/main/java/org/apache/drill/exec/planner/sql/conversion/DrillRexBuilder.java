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
package org.apache.drill.exec.planner.sql.conversion;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.util.DecimalUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;

class DrillRexBuilder extends RexBuilder {

  private static final Logger logger = LoggerFactory.getLogger(DrillRexBuilder.class);

  DrillRexBuilder(RelDataTypeFactory typeFactory) {
    super(typeFactory);
  }

  /**
   * Override makeCall to fix DECIMAL precision/scale issues in Calcite 1.38.
   * CALCITE-6427 can create invalid DECIMAL types where scale > precision.
   * This version intercepts calls WITH explicit return type.
   */
  @Override
  public RexNode makeCall(RelDataType returnType, SqlOperator op, List<RexNode> exprs) {
    // Fix DECIMAL return types for arithmetic operations
    if (returnType.getSqlTypeName() == SqlTypeName.DECIMAL) {
      int precision = returnType.getPrecision();
      int scale = returnType.getScale();

      // If scale exceeds precision, fix it
      if (scale > precision) {
        System.out.println("DrillRexBuilder.makeCall(with type): fixing invalid DECIMAL type for " + op.getName() +
                         ": precision=" + precision + ", scale=" + scale);

        // Cap precision at Drill's max (38)
        int maxPrecision = 38;
        if (precision > maxPrecision) {
          precision = maxPrecision;
        }

        // Ensure scale doesn't exceed precision
        if (scale > precision) {
          scale = precision;
        }

        System.out.println("DrillRexBuilder.makeCall(with type): corrected to precision=" + precision + ", scale=" + scale);

        // Create corrected type
        returnType = typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
      }
    }

    return super.makeCall(returnType, op, exprs);
  }

  /**
   * Override makeCall to fix DECIMAL precision/scale issues in Calcite 1.38.
   * CALCITE-6427 can create invalid DECIMAL types where scale > precision.
   * This version intercepts calls WITHOUT explicit return type (type is inferred).
   * NOTE: Cannot override makeCall(SqlOperator, RexNode...) because it's final in RexBuilder.
   * Instead, override the List version which the varargs version calls internally.
   */
  @Override
  public RexNode makeCall(SqlOperator op, List<? extends RexNode> exprs) {
    System.out.println("DrillRexBuilder.makeCall(no type): op=" + op.getName() + ", exprs=" + exprs.size());

    // Call super to get the result with inferred type
    RexNode result = super.makeCall(op, exprs);

    // Check if the inferred type has invalid DECIMAL precision/scale
    if (result.getType().getSqlTypeName() == SqlTypeName.DECIMAL) {
      int precision = result.getType().getPrecision();
      int scale = result.getType().getScale();

      System.out.println("DrillRexBuilder.makeCall(no type): inferred DECIMAL type: precision=" + precision + ", scale=" + scale);

      // If scale exceeds precision, recreate the call with fixed type
      if (scale > precision) {
        System.out.println("DrillRexBuilder.makeCall(no type): fixing invalid DECIMAL type for " + op.getName() +
                         ": precision=" + precision + ", scale=" + scale);

        // Cap precision at Drill's max (38)
        int maxPrecision = 38;
        if (precision > maxPrecision) {
          precision = maxPrecision;
        }

        // Ensure scale doesn't exceed precision
        if (scale > precision) {
          scale = precision;
        }

        System.out.println("DrillRexBuilder.makeCall(no type): corrected to precision=" + precision + ", scale=" + scale);

        // Create corrected type and recreate the call with fixed type
        RelDataType fixedType = typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
        // Convert to List<RexNode> to call the 3-arg version with explicit type
        List<RexNode> exprList = new java.util.ArrayList<>();
        for (RexNode expr : exprs) {
          exprList.add(expr);
        }
        result = super.makeCall(fixedType, op, exprList);
      }
    }

    return result;
  }

  /**
   * Since Drill has different mechanism and rules for implicit casting,
   * ensureType() is overridden to avoid conflicting cast functions being added to the expressions.
   */
  @Override
  public RexNode ensureType(
      RelDataType type,
      RexNode node,
      boolean matchNullability) {
    return node;
  }

  /**
   * Override makeCast to handle DECIMAL literal precision/scale validation.
   *
   * @param type             Type to cast to
   * @param exp              Expression being cast
   * @param matchNullability Whether to ensure the result has the same
   *                         nullability as {@code type}
   * @return Call to CAST operator
   */
  @Override
  public RexNode makeCast(RelDataType type, RexNode exp, boolean matchNullability) {
    if (matchNullability) {
      return makeAbstractCast(type, exp);
    }

    // for the case when BigDecimal literal has a scale or precision
    // that differs from the value from specified RelDataType, cast cannot be removed
    // TODO: remove this code when CALCITE-1468 is fixed
    if (type.getSqlTypeName() == SqlTypeName.DECIMAL && exp instanceof RexLiteral) {
      int precision = type.getPrecision();
      int scale = type.getScale();
      validatePrecisionAndScale(precision, scale);
      Comparable<?> value = ((RexLiteral) exp).getValueAs(Comparable.class);
      if (value instanceof BigDecimal) {
        BigDecimal bigDecimal = (BigDecimal) value;
        DecimalUtility.checkValueOverflow(bigDecimal, precision, scale);
        if (bigDecimal.precision() != precision || bigDecimal.scale() != scale) {
          return makeAbstractCast(type, exp);
        }
      }
    }
    return super.makeCast(type, exp, false);
  }

  private void validatePrecisionAndScale(int precision, int scale) {
    if (precision < 1) {
      throw UserException.validationError()
          .message("Expected precision greater than 0, but was %s.", precision)
          .build(logger);
    }
    if (scale > precision) {
      throw UserException.validationError()
          .message("Expected scale less than or equal to precision, " +
              "but was precision %s and scale %s.", precision, scale)
          .build(logger);
    }
  }
}
