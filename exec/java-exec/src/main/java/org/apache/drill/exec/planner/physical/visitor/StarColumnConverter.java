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

package org.apache.drill.exec.planner.physical.visitor;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.planner.physical.JoinPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ProjectAllowDupPrel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.util.Pair;

import com.google.common.collect.Lists;

public class StarColumnConverter extends BasePrelVisitor<Prel, boolean[], RuntimeException>{

  private static StarColumnConverter INSTANCE = new StarColumnConverter();

  private static final AtomicLong tableNumber = new AtomicLong(0);

  public static Prel insertRenameProject(Prel root, RelDataType origRowType) {
    // Insert top project to do rename only when : 1) there is a join
    // 2) there is a SCAN with * column.  We pass two boolean to keep track of
    // these two conditions.
    boolean [] renamedForStar = new boolean [2];
    renamedForStar[0] = false;
    renamedForStar[1] = false;

    //root should be screen / writer : no need to rename for the root.

    Prel child = ((Prel) root.getInput(0)).accept(INSTANCE, renamedForStar);

    if (renamedForStar[0] && renamedForStar[1]) {
      List<RexNode> exprs = Lists.newArrayList();
      for (int i = 0; i < origRowType.getFieldCount(); i++) {
        RexNode expr = child.getCluster().getRexBuilder().makeInputRef(origRowType.getFieldList().get(i).getType(), i);
        exprs.add(expr);
      }

      RelDataType newRowType = RexUtil.createStructType(child.getCluster().getTypeFactory(), exprs, origRowType.getFieldNames());

      // Insert a top project which allows duplicate columns.
      child = new ProjectAllowDupPrel(child.getCluster(), child.getTraitSet(), child, exprs, newRowType);

      List<RelNode> children = Lists.newArrayList();
      children.add( child);
      return (Prel) root.copy(root.getTraitSet(), children);

    }else{
      return root;
    }

  }


  @Override
  public Prel visitPrel(Prel prel, boolean [] renamedForStar) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for (Prel child : prel) {
      child = child.accept(this, renamedForStar);
      children.add(child);
    }

    // For project, we need make sure that the project's field name is same as the input,
    // when the project expression is RexInPutRef. This is necessary since Optiq may use
    // an arbitrary name for the project's field name.
    if (prel instanceof ProjectPrel) {
      RelNode child = children.get(0);

      List<String> fieldNames = Lists.newArrayList();

      for (Pair<String, RexNode> pair : Pair.zip(prel.getRowType().getFieldNames(), ((ProjectPrel) prel).getProjects())) {
        if (pair.right instanceof RexInputRef) {
          String name = child.getRowType().getFieldNames().get(((RexInputRef) pair.right).getIndex());
          fieldNames.add(name);
        } else {
          fieldNames.add(pair.left);
        }
      }

      // Make sure the field names are unique : Optiq does not allow duplicate field names in a rowType.
      fieldNames = makeUniqueNames(fieldNames);

      RelDataType rowType = RexUtil.createStructType(prel.getCluster().getTypeFactory(), ((ProjectPrel) prel).getProjects(), fieldNames);

      return (Prel) new ProjectPrel(prel.getCluster(), prel.getTraitSet(), children.get(0), ((ProjectPrel) prel).getProjects(), rowType);
    } else {
      return (Prel) prel.copy(prel.getTraitSet(), children);
    }
  }


  @Override
  public Prel visitJoin(JoinPrel prel, boolean [] renamedForStar) throws RuntimeException {
    renamedForStar[0] = true;    // indicate there is a join, which may require top rename projet operator.
    return visitPrel(prel, renamedForStar);
  }


  @Override
  public Prel visitScan(ScanPrel scanPrel, boolean [] renamedForStar) throws RuntimeException {
    if (StarColumnHelper.containsStarColumn(scanPrel.getRowType()) && renamedForStar[0] ) {

      renamedForStar[1] = true;  // indicate there is * for a SCAN operator.

      List<RexNode> exprs = Lists.newArrayList();

      for (RelDataTypeField field : scanPrel.getRowType().getFieldList()) {
        RexNode expr = scanPrel.getCluster().getRexBuilder().makeInputRef(field.getType(), field.getIndex());
        exprs.add(expr);
      }

      List<String> fieldNames = Lists.newArrayList();

      long tableId = tableNumber.getAndIncrement();

      for (String name : scanPrel.getRowType().getFieldNames()) {
        fieldNames.add("T" +  tableId + StarColumnHelper.PREFIX_DELIMITER + name);
      }
      RelDataType rowType = RexUtil.createStructType(scanPrel.getCluster().getTypeFactory(), exprs, fieldNames);

      ProjectPrel proj = new ProjectPrel(scanPrel.getCluster(), scanPrel.getTraitSet(), scanPrel, exprs, rowType);

      return proj;
    } else {
      return visitPrel(scanPrel, renamedForStar);
    }
  }


  private List<String> makeUniqueNames(List<String> names) {

    // We have to search the set of original names, plus the set of unique names that will be used finally .
    // Eg : the original names : ( C1, C1, C10 )
    // There are two C1, we may rename C1 to C10, however, this new name will conflict with the original C10.
    // That means we should pick a different name that does not conflict with the original names, in additional
    // to make sure it's unique in the set of unique names.

    HashSet<String> uniqueNames = new HashSet<String>();
    HashSet<String> origNames = new HashSet<String>(names);

    List<String> newNames = Lists.newArrayList();

    for (String s : names) {
      if (uniqueNames.contains(s)) {
        for (int i = 0; ; i++ ) {
          s = s + i;
          if (! origNames.contains(s) && ! uniqueNames.contains(s)) {
            break;
          }
        }
      }
      uniqueNames.add(s);
      newNames.add(s);
    }

    return newNames;
  }

}
