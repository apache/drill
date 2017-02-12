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
 ******************************************************************************/
package org.apache.drill.exec.planner.cost;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;

/**
 * Implements revised rules for the default selectivity when no
 * meta-data is available. While Calcite provides defaults, the defaults
 * are too selective and don't follow the laws of probability.
 * The revised rules are:
 *
 * <table>
 * <tr><th>Operator</th><th>Revised</th><th>Notes</th><th>Calcite Default</th></tr>
 * <tr><td>=</td><td>0.15</td><td>Default in Calcite</td><td>.15</td></tr>
 * <tr><td>&lt;></td><td>0.85</td><td>1 - p(=)</td><td>.5</td></tr>
 * <tr><td>&lt;</td><td>0.425</td><td>p(<>) / 2</td><td>.5</td></tr>
 * <tr><td>></td><td>0.425</td><td>p(<>) / 2</td><td>.5</td></tr>
 * <tr><td>&lt;=</td><td>0.575</td><td>p(<) + p(=)</td><td>.5</td></tr>
 * <tr><td>>=</td><td>0.575</td><td>p(>) + p(=)</td><td>.5</td></tr>
 * <tr><td>LIKE</td><td>0.25</td><td>Default in Calcite</td><td>.25</td></tr>
 * <tr><td>NOT LIKE</td><td>0.75</td><td>1 - p(LIKE)</td><td>.25</td></tr>
 * <tr><td>IN</td><td>0.15</td><td>IN same as =</td><td>.25</td></tr>
 * <tr><td>IN A, B, ...</td><td>max(0.5, p(=)*n - p(=)<sup>n</sup>)</td><td>Same as OR</td><td>.25 ?</td></tr>
 * <tr><td>IS NOT NULL</td><td>0.9</td><td>Default in Calcite</td><td>.9</td></tr>
 * <tr><td>IS NULL</td><td>0.1</td><td>1 - p(IS NULL)</td><td>.25 ?</td></tr>
 * <tr><td>IS TRUE</td><td>0.5</td><td>Assume Booleans not null</td><td>.25 ?</td></tr>
 * <tr><td>IS NOT TRUE</td><td>0.55</td><td>1 - p(TRUE) - p(NULL)</td><td>.25 ?</td></tr>
 * </table>
 * <p>
 * The OR operator is the sum of its operands, limited to 1.0. A series of
 * IN items is the equivalent of a series of ORs (in fact, that is how
 * Calcite implements an IN list.) The rules for IS FALSE and IS NOT FALSE
 * are the same as IS TRUE and IS NOT TRUE.
 */

public class DrillRelDefaultMdSelectivity extends RelMdSelectivity {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.SELECTIVITY.method, new DrillRelDefaultMdSelectivity());

  // Basic probabilities.
  // The first set are strongly data dependent and can be customized.

  public static final double PROB_A_EQ_B = 0.15; // From Calcite
  public static final double PROB_A_NOT_NULL = 0.9; // From Calcite
  public static final double PROB_A_IS_NULL = 1 - PROB_A_NOT_NULL;
  public static final double PROB_A_LIKE_B = 0.25; // From Calcite

  // The others are generic and are hard-coded.

  public static final double PROB_A_IS_TRUE = 0.5; // Assumes booleans are not null
  public static final double DEFAULT_PROB = 0.25; // From Calcite
  public static final double MAX_OR_FACTOR = 0.5; // From System-R, etc.

  public static final class ReductionCalculator {
    private final double probEq;   // p(a = value)
    private final double probNull; // p(a IS NULL)
    private final double probLike; // p(a LIKE value)

    public ReductionCalculator() {
      probEq = PROB_A_EQ_B;
      probNull = PROB_A_IS_NULL;
      probLike = PROB_A_LIKE_B;
    }

    public ReductionCalculator(double peq, double pnull, double plike) {
      probEq = peq;
      probNull = pnull;
      probLike = plike;
    }

    public double computeRedictionFactor(RexNode pred) {
      switch ( pred.getKind() ) {
      case BETWEEN:
        // Never called in Calcite. Drill seems to rewrite
        // a BETWEEN b AND c to a >= b AND a <= c.
        // Half the range from (-infinity, lower) or
        // half the range from (upper, infinity)
        // Bounds are inclusive
        // p(<=) / 2 = p(>=) / 2
        return (1 + probEq) / 4;
      case EQUALS:
      case IN:
        return probEq;
      case NOT_EQUALS:
        return 1 - probEq;
      case GREATER_THAN:
      case LESS_THAN:
        // p(a <> b) / 2 = (1 - prob(=)) / 2
        return (1 - probEq) / 2;
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN_OR_EQUAL:
        // p(a <> b) / 2 + prob(=) =
        // (1 - prob(=)) / 2 + prob(=)
        // (1 + prob(=)) / 2
        return (1 + probEq) / 2;
      case IS_FALSE:
      case IS_TRUE:
        // p(a IS TRUE) = p(a IS FALSE)
        return PROB_A_IS_TRUE;
      case IS_NOT_FALSE:
      case IS_NOT_TRUE:
        // p(a NOT TRUE) = p(a IS FALSE) + p(a IS NULL)
        // p(a NOT FALSE) = p(a NOT TRUE)
        // Assumes that use of this construction implies nulls
        return PROB_A_IS_TRUE + probNull;
      case IS_NULL:
        return probNull;
      case IS_NOT_NULL:
        return 1 - probNull;
      case LIKE:
        return probLike;
      case NOT:
        return 1.0 - computeRedictionFactor(((RexCall) pred).getOperands().get(0));
      case OR: {
          // p(A v B) = p(A) + p(B) - P(A ^ B)
          RexCall rexCall = (RexCall) pred;
          double sel = 0;
          double andSel = 1;
          for (RexNode op : rexCall.getOperands()) {
            double opSel = computeRedictionFactor(op);
            sel += opSel;
            andSel *= opSel;
          }

          // Per System-R and several texts, an OR (also IN) assumed to reduce
          // rows by at least 50%.
          return Math.min(MAX_OR_FACTOR, sel - andSel);
        }
      case AND: {
          // p(A ^ B) = p(A) * p(B)
          RexCall rexCall = (RexCall) pred;
          double sel = 1.0;
          for (RexNode op : rexCall.getOperands()) {
            sel *= computeRedictionFactor(op);
          }
          return sel;
        }
      default:
        return DEFAULT_PROB;
      }
    }
  }

  private static boolean useEnhancedRules = false;
  private static ReductionCalculator defaultReduction = new ReductionCalculator();

  /**
   * The enhanced rules are experimental and disabled by default.
   * They are enabled with the <tt>drill.exec.planner.defaults.enable_enhanced: true</tt>
   * boot-time config option.
   *
   * @param selection true to enable the enhanced rules
   */
  public static void useEnhancedRules(boolean selection) {
    useEnhancedRules = selection;
  }

  public static void setDefaultRules(ReductionCalculator reduction) {
    defaultReduction = reduction;
  }

  public static ReductionCalculator getReductionRules( ) { return defaultReduction; }

  // Catch-all rule when none of the others apply.
  @Override
  public Double getSelectivity(RelNode rel, RexNode predicate) {
    if (useEnhancedRules) {
      return guessSelectivity(predicate);
    } else {
      return RelMdUtil.guessSelectivity(predicate);
    }
  }

  /**
   * Returns default estimates for reduction factors, in the absence of stats.
   *
   * @param predicate      predicate for which selectivity will be computed;
   *                       null means true, so gives a reduction factor of 1.0
   * @return estimated selectivity
   */
  public  double guessSelectivity(
      RexNode predicate) {
    double sel = 1.0;
    if ((predicate == null) || predicate.isAlwaysTrue()) {
      return sel;
    }

    for (RexNode pred : RelOptUtil.conjunctions(predicate)) {
      sel *= defaultReduction.computeRedictionFactor(pred);
    }

    return sel;
  }

  // Calcite code for reference
//    if (pred.getKind() == SqlKind.IS_NOT_NULL) {
//      return .9;
//    } else if (
//        (pred instanceof RexCall)
//            && (((RexCall) pred).getOperator()
//            == RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC)) {
//      return RelMdUtil.getSelectivityValue(pred);
//    } else if (pred.isA(SqlKind.EQUALS)) {
//      return .15;
//    } else if (pred.isA(SqlKind.COMPARISON)) {
//      return .5;
//    } else {
//      return .25;
//    }

}
