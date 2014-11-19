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
package org.apache.drill.exec.planner.logical;

import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexVisitor;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.util.Util;

public class DrillPushFilterPastProjectRule extends RelOptRule {

  public final static RelOptRule INSTANCE = new DrillPushFilterPastProjectRule();

  private RexCall findItemOperator(
      final RexNode node,
      final List<RexNode> projExprs) {
    try {
      RexVisitor<Void> visitor =
          new RexVisitorImpl<Void>(true) {
        public Void visitCall(RexCall call) {
          if ("item".equals(call.getOperator().getName().toLowerCase())) {
            throw new Util.FoundOne(call); /* throw exception to interrupt tree walk (this is similar to
                                              other utility methods in RexUtil.java */
          }
          return super.visitCall(call);
        }

        public Void visitInputRef(RexInputRef inputRef) {
          final int index = inputRef.getIndex();
          RexNode n = projExprs.get(index);
          if (n instanceof RexCall) {
            RexCall r = (RexCall) n;
            if ("item".equals(r.getOperator().getName().toLowerCase())) {
              throw new Util.FoundOne(r);
            }
          }

          return super.visitInputRef(inputRef);
        }
      };
      node.accept(visitor);
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (RexCall) e.getNode();
    }
  }

  protected DrillPushFilterPastProjectRule() {
    super(
        operand(
            FilterRel.class,
            operand(ProjectRel.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    FilterRel filterRel = call.rel(0);
    ProjectRel projRel = call.rel(1);

    // Don't push Filter past Project if the Filter is referencing an ITEM expression
    // from the Project.
    //\TODO: Ideally we should split up the filter conditions into ones that
    // reference the ITEM expression and ones that don't and push the latter past the Project
    if (findItemOperator(filterRel.getCondition(), projRel.getProjects()) != null) {
      return;
    }

    // convert the filter to one that references the child of the project
    RexNode newCondition =
        RelOptUtil.pushFilterPastProject(filterRel.getCondition(), projRel);

    FilterRel newFilterRel =
        new FilterRel(
            filterRel.getCluster(),
            projRel.getChild(),
            newCondition);


    ProjectRel newProjRel =
        (ProjectRel) CalcRel.createProject(
            newFilterRel,
            projRel.getNamedProjects(),
            false);

    call.transformTo(newProjRel);
  }

}
