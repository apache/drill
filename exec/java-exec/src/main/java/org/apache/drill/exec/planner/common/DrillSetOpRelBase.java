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
package org.apache.drill.exec.planner.common;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

public abstract class DrillSetOpRelBase extends SetOp implements DrillRelNode {

  public DrillSetOpRelBase(RelOptCluster cluster, RelTraitSet traits,
    List<RelNode> inputs, SqlKind kind, boolean all, boolean checkCompatibility) throws InvalidRelException {
    super(cluster, traits, inputs, kind, all);
    if (checkCompatibility &&
      !this.isCompatible(false /* don't compare names */, true /* allow substrings */)) {
      throw new InvalidRelException(String.format("Input row types of the %s are not compatible.", kind));
    }
  }

  public boolean isCompatible(boolean compareNames, boolean allowSubstring) {
    RelDataType setOpType = getRowType();
    for (RelNode input : getInputs()) {
      if (!DrillRelOptUtil.areRowTypesCompatible(
        input.getRowType(), setOpType, compareNames, allowSubstring)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    if (kind == SqlKind.EXCEPT) {
      final List<RelNode> inputs = getInputs();
      double dRows = mq.getRowCount(inputs.get(0));
      for (int i = 1; i < inputs.size(); i++) {
        dRows -= 0.5 * mq.getRowCount(inputs.get(i));
      }
      if (dRows < 0) {
        dRows = 0;
      }
      return dRows;
    } else if (kind == SqlKind.INTERSECT) {
      double dRows = Double.MAX_VALUE;
      for (RelNode input : inputs) {
        Double rowCount = mq.getRowCount(input);
        if (rowCount == null) {
          // Assume this input does not reduce row count
          continue;
        }
        dRows = Math.min(dRows, rowCount);
      }
      dRows *= 0.25;
      return dRows;
    } else {
      double dRows = 0;
      for (RelNode input : getInputs()) {
        dRows += mq.getRowCount(input);
      }
      if (!all) {
        dRows *= 0.5;
      }
      return dRows;
    }
  }

}
