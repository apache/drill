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

import net.hydromatic.linq4j.Ord;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.work.FragmentRunnerListener;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexCorrelVariable;
import org.eigenbase.rex.RexDynamicParam;
import org.eigenbase.rex.RexFieldAccess;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexLocalRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexOver;
import org.eigenbase.rex.RexRangeRef;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.sql.SqlSyntax;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

/**
 * Utilities for Drill's planner.
 */
public class DrillOptiq {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillOptiq.class);
  
  static void registerStandardPlannerRules(RelOptPlanner planner, DrillClient client) {
    planner.addRule(EnumerableDrillRule.getInstance(client));

    // planner.addRule(DrillTableModificationConverterRule.INSTANCE);
    // planner.addRule(DrillCalcConverterRule.INSTANCE);

    planner.addRule(DrillFilterRule.INSTANCE);
    planner.addRule(DrillProjectRule.INSTANCE);
    planner.addRule(DrillAggregateRule.INSTANCE);

    // Enable when https://issues.apache.org/jira/browse/DRILL-57 fixed
    if (false) planner.addRule(DrillValuesRule.INSTANCE);
    planner.addRule(DrillSortRule.INSTANCE);
    planner.addRule(DrillJoinRule.INSTANCE);
    planner.addRule(DrillUnionRule.INSTANCE);
    // planner.addRule(AbstractConverter.ExpandConversionRule.instance);
  }

  /**
   * Converts a tree of {@link RexNode} operators into a scalar expression in Drill syntax.
   */
  static String toDrill(RelNode input, RexNode expr) {
    final RexToDrill visitor = new RexToDrill(input);
    expr.accept(visitor);
    return visitor.buf.toString();
  }

  private static class RexToDrill extends RexVisitorImpl<StringBuilder> {
    final StringBuilder buf = new StringBuilder();
    private final RelNode input;

    RexToDrill(RelNode input) {
      super(true);
      this.input = input;
    }

    @Override
    public StringBuilder visitCall(RexCall call) {
      logger.debug("RexCall {}, {}", call);
      final SqlSyntax syntax = call.getOperator().getSyntax();
      switch (syntax) {
      case Binary:
        logger.debug("Binary");
        buf.append("(");
        call.getOperands().get(0).accept(this).append(" ").append(call.getOperator().getName()).append(" ");
        return call.getOperands().get(1).accept(this).append(")");
      case Function:
        logger.debug("Function");
        buf.append(call.getOperator().getName().toLowerCase()).append("(");
        for (Ord<RexNode> operand : Ord.zip(call.getOperands())) {
          buf.append(operand.i > 0 ? ", " : "");
          operand.e.accept(this);
        }
        return buf.append(")");
      case Special:
        logger.debug("Special");
        switch (call.getKind()) {
        case Cast:
          logger.debug("Cast {}", buf);
          // Ignore casts. Drill is type-less.
          logger.debug("Ignoring cast {}, {}", call.getOperands().get(0), call.getOperands().get(0).getClass());
          return call.getOperands().get(0).accept(this);
        }
        if (call.getOperator() == SqlStdOperatorTable.itemOp) {
          final RexNode left = call.getOperands().get(0);
          final RexLiteral literal = (RexLiteral) call.getOperands().get(1);
          final String field = (String) literal.getValue2();
//          buf.append('\'');
          final int length = buf.length();
          left.accept(this);
          if (buf.length() > length) {
            // check before generating empty LHS if inputName is null
            buf.append('.');
          }
          buf.append(field);
//          buf.append('\'');
          return buf;
        }
        logger.debug("Not cast.");
        // fall through
      default:
        throw new AssertionError("todo: implement syntax " + syntax + "(" + call + ")");
      }
    }

    private StringBuilder doUnknown(Object o){
      logger.warn("Doesn't currently support consumption of {}.", o);
      return buf;
    }
    @Override
    public StringBuilder visitLocalRef(RexLocalRef localRef) {
      return doUnknown(localRef);
    }

    @Override
    public StringBuilder visitOver(RexOver over) {
      return doUnknown(over);
    }

    @Override
    public StringBuilder visitCorrelVariable(RexCorrelVariable correlVariable) {
      return doUnknown(correlVariable);
    }

    @Override
    public StringBuilder visitDynamicParam(RexDynamicParam dynamicParam) {
      return doUnknown(dynamicParam);
    }

    @Override
    public StringBuilder visitRangeRef(RexRangeRef rangeRef) {
      return doUnknown(rangeRef);
    }

    @Override
    public StringBuilder visitFieldAccess(RexFieldAccess fieldAccess) {
      return super.visitFieldAccess(fieldAccess);
    }

    @Override
    public StringBuilder visitInputRef(RexInputRef inputRef) {
      final int index = inputRef.getIndex();
      final RelDataTypeField field = input.getRowType().getFieldList().get(index);
      buf.append(field.getName());
      return buf;
    }

    @Override
    public StringBuilder visitLiteral(RexLiteral literal) {
      return buf.append(literal);
    }
  }
}
