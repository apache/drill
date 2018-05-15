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
package org.apache.drill.exec.planner.physical.visitor;

import java.util.List;

import org.apache.drill.exec.planner.physical.JoinPrel;
import org.apache.drill.exec.planner.physical.CorrelatePrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.calcite.rel.RelNode;

import com.google.common.collect.Lists;

public class JoinPrelRenameVisitor extends BasePrelVisitor<Prel, Void, RuntimeException>{

  private static JoinPrelRenameVisitor INSTANCE = new JoinPrelRenameVisitor();

  public static Prel insertRenameProject(Prel prel){
    return prel.accept(INSTANCE, null);
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    return preparePrel(prel, getChildren(prel));
  }

  private List<RelNode> getChildren(Prel prel) {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      child = child.accept(this, null);
      children.add(child);
    }
    return children;
  }

  private Prel preparePrel(Prel prel, List<RelNode> renamedNodes) {
    return (Prel) prel.copy(prel.getTraitSet(), renamedNodes);
  }

  @Override
  public Prel visitJoin(JoinPrel prel, Void value) throws RuntimeException {

    List<RelNode> children = getChildren(prel);

    final int leftCount = children.get(0).getRowType().getFieldCount();

    List<RelNode> reNamedChildren = Lists.newArrayList();

    RelNode left = prel.getJoinInput(0, children.get(0));
    RelNode right = prel.getJoinInput(leftCount, children.get(1));

    reNamedChildren.add(left);
    reNamedChildren.add(right);

    return preparePrel(prel, reNamedChildren);
  }

  //TODO: consolidate this code with join column renaming.
  @Override
  public Prel visitCorrelate(CorrelatePrel prel, Void value) throws RuntimeException {

    List<RelNode> children = getChildren(prel);

    final int leftCount = children.get(0).getRowType().getFieldCount();

    List<RelNode> reNamedChildren = Lists.newArrayList();

    RelNode left = prel.getCorrelateInput(0, children.get(0));
    RelNode right = prel.getCorrelateInput(leftCount, children.get(1));

    reNamedChildren.add(left);
    reNamedChildren.add(right);

    return preparePrel(prel, reNamedChildren);
  }
}
