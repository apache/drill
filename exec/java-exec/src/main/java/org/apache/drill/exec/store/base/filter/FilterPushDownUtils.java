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
package org.apache.drill.exec.store.base.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.util.Pair;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DateExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.IntervalDayExpression;
import org.apache.drill.common.expression.ValueExpressions.IntervalYearExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.ValueExpressions.TimeExpression;
import org.apache.drill.common.expression.ValueExpressions.TimeStampExpression;
import org.apache.drill.common.expression.ValueExpressions.VarDecimalExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.planner.PlannerPhase;

public class FilterPushDownUtils {

  /**
   * Extracted selected constants from an argument. Finds literals, omits
   * expressions, columns and so on.
   * <p>
   * The core types (INT, BIGINT, BIT (Boolean), VARCHAR and VARDECIMAL) are
   * known to work. The others may or may not work depending on Drill's
   * parser/planner; testing is needed.
   */

  private static class ConstantExtractor extends AbstractExprVisitor<ConstantHolder, Void, RuntimeException> {

    @Override
    public ConstantHolder visitIntConstant(IntExpression expr, Void value) throws RuntimeException {
      return new ConstantHolder(MinorType.INT, expr.getInt());
    }

    @Override
    public ConstantHolder visitLongConstant(LongExpression expr, Void value) throws RuntimeException {
      return new ConstantHolder(MinorType.BIGINT, expr.getLong());
    }

    @Override
    public ConstantHolder visitBooleanConstant(BooleanExpression expr, Void value) throws RuntimeException {
      return new ConstantHolder(MinorType.BIT, expr.getBoolean());
    }

    @Override
    public ConstantHolder visitQuotedStringConstant(QuotedString expr, Void value) throws RuntimeException {
      return new ConstantHolder(MinorType.VARCHAR, expr.getString());
    }

    // Float mapped to Double for storage to simplify clients
    // Not clear that Drill generates floats rather than doubles.
    @Override
    public ConstantHolder visitFloatConstant(FloatExpression expr, Void value) throws RuntimeException {
      return new ConstantHolder(MinorType.FLOAT8, (double) expr.getFloat());
    }

    // Seems to not be used. Anything float-like is instead represented as a
    // VarDecimal constant.
    @Override
    public ConstantHolder visitDoubleConstant(DoubleExpression expr, Void value) throws RuntimeException {
      return new ConstantHolder(MinorType.FLOAT8, expr.getDouble());
    }

    // Legacy decimals no longer supported, so not implemented.

    @Override
    public ConstantHolder visitVarDecimalConstant(VarDecimalExpression expr, Void value) throws RuntimeException {
      return new ConstantHolder(MinorType.VARDECIMAL, expr.getBigDecimal());
    }

    // Example: DATE '2008-2-23'
    @Override
    public ConstantHolder visitDateConstant(DateExpression expr, Void value) throws RuntimeException {
      return new ConstantHolder(MinorType.DATE, expr.getDate());
    }

    // Example: TIME '12:23:34'
    @Override
    public ConstantHolder visitTimeConstant(TimeExpression expr, Void value) throws RuntimeException {
      return new ConstantHolder(MinorType.TIME, expr.getTime());
    }

    // Example: TIMESTAMP '2008-2-23 12:23:34.456'
    @Override
    public ConstantHolder visitTimeStampConstant(TimeStampExpression expr, Void value) throws RuntimeException {
      return new ConstantHolder(MinorType.TIMESTAMP, expr.getTimeStamp());
    }

    // Example: INTERVAL '1' YEAR
    @Override
    public ConstantHolder visitIntervalYearConstant(IntervalYearExpression expr, Void value) throws RuntimeException {
      return new ConstantHolder(MinorType.INTERVALYEAR, expr.getIntervalYear());
    }

    // Example: INTERVAL '1 10:20:30' DAY TO SECOND
    // This field has two parts, encoded as a Pair.
    @Override
    public ConstantHolder visitIntervalDayConstant(IntervalDayExpression expr, Void value) throws RuntimeException {
      return new ConstantHolder(MinorType.INTERVALDAY,
          Pair.of(expr.getIntervalDay(), expr.getIntervalMillis()));
    }

   @Override
    public ConstantHolder visitUnknown(LogicalExpression e, Void valueArg) throws RuntimeException {
      return null;
    }
  }

  /**
   * Extract a column name argument, or null if the argument is not a column, or is
   * a complex column (a[10], a.b).
   */

  private static class ColRefExtractor extends AbstractExprVisitor<String, Void, RuntimeException> {

    @Override
    public String visitSchemaPath(SchemaPath path, Void value) throws RuntimeException {

      // Can't handle names such as a.b or a[10]

      if (! path.isLeaf()) {
        return null;
      }

      // Can only handle columns known to the scan

      return path.getRootSegmentPath();
    }

    @Override
    public String visitUnknown(LogicalExpression e, Void valueArg) throws RuntimeException {
      return null;
    }
  }

  /**
   * Extract a relational operator of the pattern<br>
   * <tt>&lt;col> &lt;relop> &lt;const></tt> or<br>
   * <tt>&lt;col> &lt;relop></tt>.
   */

  private static class RelOpExtractor extends AbstractExprVisitor<List<RelOp>, Void, RuntimeException> {

    @Override
    public List<RelOp> visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
      switch (op.getName()) {
      case BooleanOperator.OR_FN:
        break;
      case BooleanOperator.AND_FN:
        assert false : "Should not get here, the CNF conversion should have handled AND";
      default:
        return null;
      }

      // OR is allowed only when the equivalent of IN:
      // a IN('x', 'y') is equivalent to a = 'x' OR a = 'y'
      // a IN('x', 'y', 'z') is equivalent to a = 'z' OR (a = 'y' OR a = 'z')

      List<RelOp> left = op.args.get(0).accept(this, null);
      if (left == null) {
        return null;
      }
      List<RelOp> right = op.args.get(1).accept(this, null);
      if (right == null) {
        return null;
      }
      List<RelOp> scans = new ArrayList<>();
      scans.addAll(left);
      scans.addAll(right);
      return scans;
    }


    @Override
    public List<RelOp> visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {

      RelOp.Op op;
      switch(call.getName()) {
      case FunctionCall.EQ_FN:
        op = RelOp.Op.EQ;
        break;
      case FunctionCall.NE_FN:
        op = RelOp.Op.NE;
        break;
      case FunctionCall.LT_FN:
        op = RelOp.Op.LT;
        break;
      case FunctionCall.LE_FN:
        op = RelOp.Op.LE;
        break;
      case FunctionCall.GT_FN:
        op = RelOp.Op.GT;
        break;
      case FunctionCall.GE_FN:
        op = RelOp.Op.GE;
        break;
      case FunctionCall.IS_NULL:
        op = RelOp.Op.IS_NULL;
        break;
      case FunctionCall.IS_NOT_NULL:
        op = RelOp.Op.IS_NOT_NULL;
        break;
      default:
        return null;
      }

      RelOp relOp;
      if (op.argCount() == 1) {
        relOp = checkCol(op, call);
      } else {
        relOp = checkColOpConst(op, call);
        if (relOp == null) {
          relOp = checkConstOpCol(op, call);
        }
      }
      return relOp == null ? null : Collections.singletonList(relOp);
    }

    /**
     * Check just the one argument for a unary operator:
     * IS NULL, IS NOT NULL.
     */

    private RelOp checkCol(RelOp.Op op, FunctionCall call) {
      String colName = call.args.get(0).accept(COL_REF_EXTRACTOR, null);
      if (colName == null) {
        return null;
      }

      return new RelOp(op, colName, null);
    }

    /**
     * Extracts a relational operator of the "normal" form of:<br>
     * <tt>&lt;col> &lt;relop> &lt;const>.
     */

    private RelOp checkColOpConst(RelOp.Op op, FunctionCall call) {
      String colName = call.args.get(0).accept(COL_REF_EXTRACTOR, null);
      if (colName == null) {
        return null;
      }

      ConstantHolder constArg = call.args.get(1).accept(CONSTANT_EXTRACTOR, null);
      if (constArg == null) {
        return null;
      }

      return new RelOp(op, colName, constArg);
    }

    /**
     * Extracts a relational operator of the "reversed" form of:<br>
     * <tt>&lt;const> &lt;relop> &lt;col>. (Unfortunately, Calcite
     * does not normalize predicates.) Reverses the sense of the
     * relational operator to put the predicate into normalized
     * form.
     */

    private RelOp checkConstOpCol(RelOp.Op op, FunctionCall call) {
      ConstantHolder constArg = call.args.get(0).accept(CONSTANT_EXTRACTOR, null);
      if (constArg == null) {
        return null;
      }

      String colName = call.args.get(1).accept(COL_REF_EXTRACTOR, null);
      if (colName == null) {
        return null;
      }

      return new RelOp(op.invert(), colName, constArg);
    }

    @Override
    public List<RelOp> visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
      // Catches OR clauses among other things
      return null;
    }
  }

  private static final ConstantExtractor CONSTANT_EXTRACTOR = new ConstantExtractor();

  private static final ColRefExtractor COL_REF_EXTRACTOR = new ColRefExtractor();

  public static final RelOpExtractor REL_OP_EXTRACTOR = new RelOpExtractor();

  /**
   * Filter push-down is best done during logical planning so that the result can
   * influence parallelization in the physical phase. The specific phase differs
   * depending on which planning mode is enabled. This check hides those details
   * from storage plugins that simply want to know "should I add my filter
   * push-down rules in the given phase?"
   *
   * @return true if filter push-down rules should be applied in this phase
   */
  public static boolean isFilterPushDownPhase(PlannerPhase phase) {
    switch (phase) {
    case LOGICAL_PRUNE_AND_JOIN: // HEP is disabled
    case PARTITION_PRUNING:      // HEP partition push-down enabled
    case LOGICAL_PRUNE:          // HEP partition push-down disabled
      return true;
    default:
      return false;
    }
  }
}
