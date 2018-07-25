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

import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.planner.physical.LateralJoinPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.UnnestPrel;

import java.util.List;

/**
 * LateralUnnestRowIDVisitor traverses the physical plan and modifies all the operators in the
 * pipeline of Lateral and Unnest operators to accommodate IMPLICIT_COLUMN. The source for the
 * IMPLICIT_COLUMN is unnest operator and the sink for the column is the corresponding Lateral
 * join operator.
 */
public class LateralUnnestRowIDVisitor extends BasePrelVisitor<Prel, Boolean, RuntimeException> {

  private static LateralUnnestRowIDVisitor INSTANCE = new LateralUnnestRowIDVisitor();

  public static Prel insertRowID(Prel prel){
    return prel.accept(INSTANCE, false);
  }

  @Override
  public Prel visitPrel(Prel prel, Boolean isRightOfLateral) throws RuntimeException {
    List<RelNode> children = getChildren(prel, isRightOfLateral);
    if (isRightOfLateral) {
      return prel.addImplicitRowIDCol(children);
    } else {
      return (Prel) prel.copy(prel.getTraitSet(), children);
    }
  }

  private List<RelNode> getChildren(Prel prel, Boolean isRightOfLateral) {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      child = child.accept(this, isRightOfLateral);
      children.add(child);
    }
    return children;
  }

  @Override
  public Prel visitLateral(LateralJoinPrel prel, Boolean value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    children.add(((Prel)prel.getInput(0)).accept(this, false));
    children.add(((Prel) prel.getInput(1)).accept(this, true));

    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitUnnest(UnnestPrel prel, Boolean value) throws RuntimeException {
    return prel.addImplicitRowIDCol(null);
  }
}
