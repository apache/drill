/*******************************************************************************
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
package org.apache.drill.optiq;

import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlSyntax;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

/**
 * Utilities for Drill's planner.
 */
public class DrillOptiq {
  static void registerStandardPlannerRules(RelOptPlanner planner) {
    planner.addRule(EnumerableDrillRule.ARRAY_INSTANCE);
    planner.addRule(EnumerableDrillRule.CUSTOM_INSTANCE);

//    planner.addRule(DrillTableModificationConverterRule.INSTANCE);
//    planner.addRule(DrillAggregateConverterRule.INSTANCE);
//    planner.addRule(DrillCalcConverterRule.INSTANCE);

    planner.addRule(DrillFilterRule.INSTANCE);
    planner.addRule(DrillProjectRule.INSTANCE);

    // Enable when https://issues.apache.org/jira/browse/DRILL-57 fixed
    if (false) planner.addRule(DrillValuesRule.INSTANCE);
//    planner.addRule(DrillSortRule.INSTANCE);
//    planner.addRule(DrillJoinRule.INSTANCE);
//    planner.addRule(DrillUnionRule.INSTANCE);
//    planner.addRule(AbstractConverter.ExpandConversionRule.instance);
  }

  /** Converts a tree of {@link RexNode} operators into a scalar expression in
   * Drill syntax. */
  static String toDrill(RexNode expr, String inputName) {
    final RexToDrill visitor = new RexToDrill(inputName);
    expr.accept(visitor);
    String s = visitor.buf.toString();
    return s;
  }

  private static class RexToDrill extends RexVisitorImpl<StringBuilder> {
    final StringBuilder buf = new StringBuilder();
    private final String inputName;

    RexToDrill(String inputName) {
      super(true);
      this.inputName = inputName;
    }

    @Override
    public StringBuilder visitCall(RexCall call) {
      final SqlSyntax syntax = call.getOperator().getSyntax();
      switch (syntax) {
      case Binary:
        buf.append("(");
        call.getOperandList().get(0).accept(this)
            .append(" ")
            .append(call.getOperator().getName())
            .append(" ");
        return call.getOperandList().get(1).accept(this)
            .append(")");
      case Special:
        switch (call.getKind()) {
        case Cast:
          // Ignore casts. Drill is type-less.
          return call.getOperandList().get(0).accept(this);
        }
        if (call.getOperator() == SqlStdOperatorTable.itemOp) {
          final RexNode left = call.getOperandList().get(0);
          final RexLiteral literal = (RexLiteral) call.getOperandList().get(1);
          final String field = (String) literal.getValue2();
          final int length = buf.length();
          left.accept(this);
          if (buf.length() > length) {
            // check before generating empty LHS if inputName is null
            buf.append('.');
          }
          return buf.append(field);
        }
        // fall through
      default:
        throw new AssertionError("todo: implement syntax " + syntax + "(" + call
            + ")");
      }
    }

    @Override
    public StringBuilder visitInputRef(RexInputRef inputRef) {
      assert inputRef.getIndex() == 0;
      if (inputName == null) {
        return buf;
      }
      return buf.append(inputName);
    }

    @Override
    public StringBuilder visitLiteral(RexLiteral literal) {
      return buf.append(literal);
    }
  }
}

// End DrillOptiq.java
