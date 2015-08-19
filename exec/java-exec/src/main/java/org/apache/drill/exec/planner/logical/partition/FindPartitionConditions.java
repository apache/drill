/**
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
package org.apache.drill.exec.planner.logical.partition;

import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.util.Util;

import com.google.common.collect.Lists;


public class FindPartitionConditions extends RexVisitorImpl<Void> {
  /** Whether an expression is a directory filter, and if so, whether
   * it can be pushed into the scan.
   */
  enum PushDirFilter {
    NO_PUSH, PUSH
  }

  /**
   * During top-down traversal of the expression tree, keep track of the
   * boolean operators such that if a directory filter is found, it will
   * be added as a child of the current boolean operator.
   *
   * NOTE: this auxiliary class is necessary because RexNodes are immutable.
   * If they were mutable, we could have easily added/dropped inputs as we
   * encountered directory filters.
   */
  public class BooleanOpState {
    private SqlOperator booleanOp;
    private List<RexNode> children = Lists.newArrayList();
    public BooleanOpState(SqlOperator op) {
      booleanOp = op;
    }
    public SqlOperator getOp() {
      return booleanOp;
    }
    public void addChild(RexNode n) {
      if (!children.contains(n)) {
        children.add(n);
      }
    }
    public List<RexNode> getChildren() {
      return children;
    }
    public void clear() {
      children.clear();
    }
  }

  private final BitSet dirs;

  private final List<PushDirFilter> pushStatusStack =  Lists.newArrayList();
  private final Deque<SqlOperator> parentCallTypeStack = new ArrayDeque<SqlOperator>();
  private final Deque<BooleanOpState> opStack = new ArrayDeque<BooleanOpState>();

  private RexBuilder builder = null;
  private RexNode resultCondition = null;

  public FindPartitionConditions(BitSet dirs) {
    // go deep
    super(true);
    this.dirs = dirs;
  }

  public FindPartitionConditions(BitSet dirs, RexBuilder builder) {
    // go deep
    super(true);
    this.dirs = dirs;
    this.builder = builder;
  }

  public void analyze(RexNode exp) {
    assert pushStatusStack.isEmpty();

    exp.accept(this);

    // Deal with top of stack
    assert pushStatusStack.size() == 1;
    assert parentCallTypeStack.isEmpty();
    PushDirFilter rootPushDirFilter = pushStatusStack.get(0);
    if (rootPushDirFilter == PushDirFilter.PUSH) {
      // The entire subtree was directory filter, so add it to the result.
      addResult(exp);
    }
    pushStatusStack.clear();
  }

  public RexNode getFinalCondition() {
    return resultCondition;
  }

  private Void pushVariable() {
    pushStatusStack.add(PushDirFilter.NO_PUSH);
    return null;
  }

  private void addResult(RexNode exp) {
    // when we find a directory filter, add it to the current boolean operator's
    // children (if one exists)
    if (!opStack.isEmpty()) {
      BooleanOpState op = opStack.peek();
      op.addChild(exp);
    } else {
      resultCondition = exp;
    }
  }

  /**
   * For an OR node that is marked as NO_PUSH, there could be 3 situations:
   * 1. left child has a partition condition, right child does not.  In this case, we should not push any child of this OR
   * 2. left child does not have partition condition, right child has one.  Again, we should not push any child of this OR
   * 3. left and right child both have partition condition but both sides may have had other non-partition conditions. In
   *    this case, we can push the partition conditions by building a new OR combining both children.
   * In this method we clear the children of the OR for cases 1 and 2 and leave it alone for case 3
   */
  private void clearOrChildrenIfSingle() {
    if (!opStack.isEmpty()) {
      BooleanOpState op = opStack.peek();
      assert op.getOp().getKind() == SqlKind.OR;
      if (op.getChildren().size() == 1) {
        op.clear();
      }
    }
  }

  /**
   * If the top of the parentCallTypeStack is an AND or OR, get the corresponding
   * top item from the BooleanOpState stack and examine its children - these must
   * be the directory filters we are interested in.  Create a new filter condition
   * using the boolean operation and the children. Add this new filter as a child
   * of the parent boolean operator - thus the filter condition gets built bottom-up.
   */
  private void popAndBuildFilter() {
    SqlOperator op1 = null;
    if (!parentCallTypeStack.isEmpty()) {
      op1 = parentCallTypeStack.pop();
    }
    if (op1 != null
        && (op1.getKind() == SqlKind.AND || op1.getKind() == SqlKind.OR)
        && !opStack.isEmpty()) {
      BooleanOpState op = opStack.pop();
      int size = op.getChildren().size();
      RexNode newFilter = null;
      if (size > 1) {
        newFilter = builder.makeCall(op.getOp(),  op.getChildren());
      } else if (size == 1) {
        newFilter = op.getChildren().get(0);
      }
      if (newFilter != null) {
        // add this new filter to my parent boolean operator's children
        if (!opStack.isEmpty()) {
          op = opStack.peek();
          op.addChild(newFilter);
        } else {
          resultCondition = newFilter;
        }
      }
    }
  }


  public Void visitInputRef(RexInputRef inputRef) {
    if(dirs.get(inputRef.getIndex())){
      pushStatusStack.add(PushDirFilter.PUSH);
    }else{
      pushStatusStack.add(PushDirFilter.NO_PUSH);
    }
    return null;
  }

  public Void visitLiteral(RexLiteral literal) {
    pushStatusStack.add(PushDirFilter.PUSH);
    return null;
  }

  public Void visitOver(RexOver over) {
    // assume NO_PUSH until proven otherwise
    analyzeCall(over, PushDirFilter.NO_PUSH);
    return null;
  }

  public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
    return pushVariable();
  }

  public Void visitCall(RexCall call) {
    boolean visited = false;
    // examine the input of a CAST function; this could be extended for
    // other functions in the future.
    if (call.getOperator().getSyntax() == SqlSyntax.SPECIAL &&
        call.getKind() == SqlKind.CAST) {
      RexNode n = call.getOperands().get(0);
      if (n instanceof RexInputRef) {
        visitInputRef((RexInputRef) n);
        visited = true;
      }
    }
    if (!visited) {
      // assume PUSH until proven otherwise
      analyzeCall(call, PushDirFilter.PUSH);
    }
    return null;
  }

  private void analyzeCall(RexCall call, PushDirFilter callPushDirFilter) {
    parentCallTypeStack.push(call.getOperator());
    if (call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR) {
      opStack.push(new BooleanOpState(call.getOperator()));
    }

    // visit operands, pushing their states onto stack
    super.visitCall(call);

    // look for NO_PUSH operands
    int operandCount = call.getOperands().size();
    List<PushDirFilter> operandStack = Util.last(pushStatusStack, operandCount);
    for (PushDirFilter operandPushDirFilter : operandStack) {
      if (operandPushDirFilter == PushDirFilter.NO_PUSH) {
        callPushDirFilter = PushDirFilter.NO_PUSH;
      }
    }

    // Even if all operands are PUSH, the call itself may
    // be non-deterministic.
    if (!call.getOperator().isDeterministic()) {
      callPushDirFilter = PushDirFilter.NO_PUSH;
    } else if (call.getOperator().isDynamicFunction()) {
      // For now, treat it same as non-deterministic.
      callPushDirFilter = PushDirFilter.NO_PUSH;
    }

    // Row operator itself can't be reduced to a PUSH
    if ((callPushDirFilter == PushDirFilter.PUSH)
        && (call.getOperator() instanceof SqlRowOperator)) {
      callPushDirFilter = PushDirFilter.NO_PUSH;
    }


    if (callPushDirFilter == PushDirFilter.NO_PUSH) {
      if (call.getKind() == SqlKind.AND) {
        // one or more children is not a push-able directory filter. If this is an AND, add
        // all the ones that are push-able directory filters.
        for (int iOperand = 0; iOperand < operandCount; ++iOperand) {
          PushDirFilter pushDirFilter = operandStack.get(iOperand);
          RexNode n = call.getOperands().get(iOperand);
          if (pushDirFilter == PushDirFilter.PUSH && !(n.getKind() == SqlKind.AND || n.getKind() == SqlKind.OR)) {
            addResult(n);
          }
        }
      } else if (call.getKind() == SqlKind.OR) {
        clearOrChildrenIfSingle();
      }
    }
    else if (callPushDirFilter == PushDirFilter.PUSH && !(call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR)) {
      addResult(call);
    }

    // pop operands off of the stack
    operandStack.clear();

    // pop this parent call operator off the stack and build the intermediate filters as we go
    popAndBuildFilter();

    // push PushDirFilter result for this call onto stack
    pushStatusStack.add(callPushDirFilter);
  }

  public Void visitDynamicParam(RexDynamicParam dynamicParam) {
    return pushVariable();
  }

  public Void visitRangeRef(RexRangeRef rangeRef) {
    return pushVariable();
  }

  public Void visitFieldAccess(RexFieldAccess fieldAccess) {
    return pushVariable();
  }


}
