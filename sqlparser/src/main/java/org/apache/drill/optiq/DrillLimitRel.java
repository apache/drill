package org.apache.drill.optiq;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;

import java.util.List;

public class DrillLimitRel extends SingleRel implements DrillRel {
  private RexNode offset;
  private RexNode fetch;

  public DrillLimitRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, child);
    this.offset = offset;
    this.fetch = fetch;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillLimitRel(getCluster(), traitSet, sole(inputs), offset, fetch);
  }

  @Override
  public int implement(DrillImplementor implementor) {
    int inputId = implementor.visitChild(this, 0, getChild());
    final ObjectNode limit = implementor.mapper.createObjectNode();
    limit.put("op", "limit");
    limit.put("input", inputId);
    int offsetVal = offset != null ? Math.max(0, RexLiteral.intValue(offset)) : 0;
    // First offset to include into results (inclusive). Null implies it is starting from offset 0
    limit.put("first", offsetVal);
    // Last offset to stop including into results (exclusive), translating fetch row counts into an offset.
    // Null value implies including entire remaining result set from first offset
    limit.put("last", fetch != null ? Math.max(0, RexLiteral.intValue(fetch)) + offsetVal : null);
    return implementor.add(limit);
  }

}
