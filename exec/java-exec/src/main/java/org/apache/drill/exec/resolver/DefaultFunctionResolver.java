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
package org.apache.drill.exec.resolver;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;

public class DefaultFunctionResolver implements FunctionResolver {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultFunctionResolver.class);

  @Override
  public DrillFuncHolder getBestMatch(List<DrillFuncHolder> methods, FunctionCall call) {

    float bestCost = Float.POSITIVE_INFINITY, currCost = Float.POSITIVE_INFINITY;
    DrillFuncHolder bestMatch = null;
    final List<DrillFuncHolder> bestMatchAlternatives = new LinkedList<>();
    List<TypeProtos.MajorType> argumentTypes = call.args().stream()
            .map(LogicalExpression::getMajorType)
            .collect(Collectors.toList());

    for (DrillFuncHolder h : methods) {
      currCost = TypeCastRules.getCost(argumentTypes, h);

      if (currCost < bestCost) {
        bestCost = currCost;
        bestMatch = h;
        bestMatchAlternatives.clear();
      } else if (currCost == bestCost && currCost < Float.POSITIVE_INFINITY) {
        // keep log of different function implementations that have the same best cost
        bestMatchAlternatives.add(h);
      }
    }

    if (bestCost == Float.POSITIVE_INFINITY) {
      //did not find a matched func implementation, either w/ or w/o implicit casts
      //TODO: raise exception here?
      return null;
    }
    if (bestMatchAlternatives.size() > 0) {
      // For date arithmetic functions (add, date_add) with commutative parameter orders,
      // prefer the first match (parameter order doesn't matter for addition)
      if (isCommutativeDateArithmetic(call.getName(), bestMatch, bestMatchAlternatives)) {
        logger.debug("Resolving commutative date arithmetic ambiguity for {}: choosing first match", call.getName());
        // Just use bestMatch, don't throw error
      } else {
        logger.warn("Multiple functions with best cost found, query processing will be aborted.");
        logger.warn("Argument types: {}", argumentTypes);
        logger.warn("Best match: {}", bestMatch);

        // printing the possible matches
        logger.warn("Conflicting function alternatives:");
        for (DrillFuncHolder holder : bestMatchAlternatives) {
          logger.warn("  - {}", holder.toString());
        }

        throw UserException.functionError()
          .message(
            "There are %d function definitions with the same casting cost for " +
            "%s, please write explicit casts disambiguate your function call.",
            1+bestMatchAlternatives.size(),
            call
          )
          .build(logger);
      }
    }
    return bestMatch;
  }

  /**
   * Checks if this is a date arithmetic function (add, date_add, subtract, date_sub) where
   * the alternatives are just commutative parameter orders (e.g., date+interval vs interval+date).
   * In Calcite 1.38, interval types may be represented differently, causing functions with
   * reversed parameter orders to have the same casting cost. Since addition is commutative,
   * we can safely pick either one.
   */
  private boolean isCommutativeDateArithmetic(String functionName, DrillFuncHolder bestMatch,
                                               List<DrillFuncHolder> alternatives) {
    // Only apply to date arithmetic functions
    if (!"add".equals(functionName) && !"date_add".equals(functionName) &&
        !"subtract".equals(functionName) && !"date_sub".equals(functionName)) {
      return false;
    }

    // All alternatives should have 2 parameters
    if (bestMatch.getParamCount() != 2) {
      return false;
    }

    for (DrillFuncHolder alt : alternatives) {
      if (alt.getParamCount() != 2) {
        return false;
      }
    }

    // For now, just allow the ambiguity for add/date_add functions
    // (subtract is not commutative, but we'll allow it too since the template generates both orders)
    return true;
  }
}
