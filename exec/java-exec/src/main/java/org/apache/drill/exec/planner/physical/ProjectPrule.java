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
package org.apache.drill.exec.planner.physical;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.hydromatic.linq4j.Ord;

import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionType;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.relopt.volcano.RelSubset;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlKind;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class ProjectPrule extends RelOptRule {
  public static final RelOptRule INSTANCE = new ProjectPrule();

  private ProjectPrule() {
    super(RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(RelNode.class)), "ProjectPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillProjectRel project = (DrillProjectRel) call.rel(0);
    final RelNode input = call.rel(1);

    RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL);
    RelNode convertedInput = convert(input, traits);
    
    Map<Integer, Integer> inToOut = getProjectMap(project);
    
    if (convertedInput instanceof RelSubset) {
      RelSubset subset = (RelSubset) convertedInput;
      for (RelNode rel : subset.getRelList()) {
        if (!rel.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE).equals(DrillDistributionTrait.DEFAULT)) {
          DrillDistributionTrait childDist = rel.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE);
          RelCollation childCollation = rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
          
          
          DrillDistributionTrait newDist = convertDist(childDist, inToOut);
          RelCollation newCollation = convertRelCollation(childCollation, inToOut);
          
          call.transformTo(new ProjectPrel(project.getCluster(), project.getTraitSet().plus(newDist).plus(newCollation).plus(Prel.DRILL_PHYSICAL), 
              rel, project.getProjects(), project.getRowType()));
        }
      }
      
    } else{
      call.transformTo(new ProjectPrel(project.getCluster(), convertedInput.getTraitSet(), convertedInput, project.getProjects(), project.getRowType()));        
    }
  }
  
  private DrillDistributionTrait convertDist(DrillDistributionTrait srcDist, Map<Integer, Integer> inToOut) {
    List<DistributionField> newFields = Lists.newArrayList();
    
    for (DistributionField field : srcDist.getFields()) {
      if (inToOut.containsKey(field.getFieldId())) {
        newFields.add(new DistributionField(inToOut.get(field.getFieldId())));
      }
    }
    
    if (newFields.isEmpty()) {
      if (srcDist.getType() != DistributionType.SINGLETON) {
        return DrillDistributionTrait.RANDOM_DISTRIBUTED;
      } else {
        return DrillDistributionTrait.SINGLETON;
      }
    } else {
      return new DrillDistributionTrait(srcDist.getType(), ImmutableList.copyOf(newFields));
    }
  }

  private RelCollation convertRelCollation(RelCollation src, Map<Integer, Integer> inToOut) {
    List<RelFieldCollation> newFields = Lists.newArrayList();
    
    for ( RelFieldCollation field : src.getFieldCollations()) {
      if (inToOut.containsKey(field.getFieldIndex())) {
        newFields.add(new RelFieldCollation(inToOut.get(field.getFieldIndex())));
      }
    }
    
    if (newFields.isEmpty()) {
      return RelCollationImpl.EMPTY;
    } else {
      return RelCollationImpl.of(newFields);
    }
  }
  
  private Map<Integer, Integer> getProjectMap(DrillProjectRel project) {
    Map<Integer, Integer> m = new HashMap<Integer, Integer>();
    
    for (Ord<RexNode> node : Ord.zip(project.getProjects())) {
      if (node.e instanceof RexInputRef) {
        m.put( ((RexInputRef) node.e).getIndex(), node.i);
      } else if (node.e.isA(SqlKind.CAST)) {
        RexNode operand = ((RexCall) node.e).getOperands().get(0);
        if (operand instanceof RexInputRef) {
          m.put(              
              ((RexInputRef) operand).getIndex(), node.i);
        }
      }
    }
    return m;
    
  }
    
}
