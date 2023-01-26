package org.apache.drill.exec.planner.common;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public interface DrillSetOpRel {
  default boolean isCompatible(RelDataType setOpType, List<RelNode> inputs) {
    for (RelNode input : inputs) {
      if (!DrillRelOptUtil.areRowTypesCompatible(
        input.getRowType(), setOpType, false, true)) {
        return false;
      }
    }
    return true;
  }
}
