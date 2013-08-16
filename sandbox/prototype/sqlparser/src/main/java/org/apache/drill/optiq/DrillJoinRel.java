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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Pair;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Join implemented in Drill.
 */
public class DrillJoinRel extends JoinRelBase implements DrillRel {
  private final List<Integer> leftKeys = new ArrayList<>();
  private final List<Integer> rightKeys = new ArrayList<>();

  /** Creates a DrillJoinRel. */
  public DrillJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType) throws InvalidRelException {
    super(cluster, traits, left, right, condition, joinType, Collections.<String> emptySet());
    switch (joinType) {
    case RIGHT:
      throw new InvalidRelException("DrillJoinRel does not support RIGHT join");
    }
    RexNode remaining = RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys);
    if (!remaining.isAlwaysTrue()) {
      throw new InvalidRelException("DrillJoinRel only supports equi-join");
    }
  }

  @Override
  public DrillJoinRel copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right) {
    try {
      return new DrillJoinRel(getCluster(), traitSet, left, right, condition, joinType);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public int implement(DrillImplementor implementor) {
    final List<String> fields = getRowType().getFieldNames();
    assert isUnique(fields);
    final int leftCount = left.getRowType().getFieldCount();
    final List<String> leftFields = fields.subList(0, leftCount);
    final List<String> rightFields = fields.subList(leftCount, fields.size());

    final int leftId = implementInput(implementor, 0, 0, left);
    final int rightId = implementInput(implementor, 1, leftCount, right);

    /*
     * E.g. { op: "join", left: 2, right: 4, conditions: [ {relationship: "==", left: "deptId", right: "deptId"} ] }
     */
    final ObjectNode join = implementor.mapper.createObjectNode();
    join.put("op", "join");
    join.put("left", leftId);
    join.put("right", rightId);
    join.put("type", toDrill(joinType));
    final ArrayNode conditions = implementor.mapper.createArrayNode();
    join.put("conditions", conditions);
    for (Pair<Integer, Integer> pair : Pair.zip(leftKeys, rightKeys)) {
      final ObjectNode condition = implementor.mapper.createObjectNode();
      condition.put("relationship", "==");
      condition.put("left", leftFields.get(pair.left));
      condition.put("right", rightFields.get(pair.right));
      conditions.add(condition);
    }
    return implementor.add(join);
  }

  private int implementInput(DrillImplementor implementor, int i, int offset, RelNode input) {
    final int inputId = implementor.visitChild(this, i, input);
    assert uniqueFieldNames(input.getRowType());
    final List<String> fields = getRowType().getFieldNames();
    final List<String> inputFields = input.getRowType().getFieldNames();
    final List<String> outputFields = fields.subList(offset, offset + inputFields.size());
    if (!outputFields.equals(inputFields)) {
      // Ensure that input field names are the same as output field names.
      // If there are duplicate field names on left and right, fields will get
      // lost.
      return rename(implementor, inputId, inputFields, outputFields);
    } else {
      return inputId;
    }
  }

  private int rename(DrillImplementor implementor, int inputId, List<String> inputFields, List<String> outputFields) {
    final ObjectNode project = implementor.mapper.createObjectNode();
    project.put("op", "project");
    project.put("input", inputId);
    final ArrayNode transforms = implementor.mapper.createArrayNode();
    project.put("projections", transforms);
    for (Pair<String, String> pair : Pair.zip(inputFields, outputFields)) {
      final ObjectNode objectNode = implementor.mapper.createObjectNode();
      transforms.add(objectNode);
      objectNode.put("expr", pair.left);
//      objectNode.put("ref", "output." + pair.right);
      objectNode.put("ref", pair.right);
    }
    return implementor.add(project);
  }

  /**
   * Returns whether there are any elements in common between left and right.
   */
  private static <T> boolean intersects(List<T> left, List<T> right) {
    return new HashSet<>(left).removeAll(right);
  }

  private boolean uniqueFieldNames(RelDataType rowType) {
    return isUnique(rowType.getFieldNames());
  }

  private static <T> boolean isUnique(List<T> list) {
    return new HashSet<>(list).size() == list.size();
  }

  private static String toDrill(JoinRelType joinType) {
    switch (joinType) {
    case LEFT:
      return "left";
    case INNER:
      return "inner";
    case FULL:
      return "outer";
    default:
      throw new AssertionError(joinType);
    }
  }
}
