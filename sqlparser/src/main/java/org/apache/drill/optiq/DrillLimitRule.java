package org.apache.drill.optiq;

import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

public class DrillLimitRule extends RelOptRule {
  public static DrillLimitRule INSTANCE = new DrillLimitRule();

  private DrillLimitRule() {
    super(RelOptRule.some(SortRel.class, Convention.NONE, RelOptRule.any(RelNode.class)), "DrillLimitRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final SortRel sort = call.rel(0);
    if (sort.offset == null && sort.fetch == null) {
      return;
    }
    final RelTraitSet traits = sort.getTraitSet().plus(DrillRel.CONVENTION);
    RelNode input = sort.getChild();
    //RelNode input = sort.getChild();
    if (!sort.getCollation().getFieldCollations().isEmpty()) {
      input = sort.copy(
          sort.getTraitSet().replace(RelCollationImpl.EMPTY),
          input,
          RelCollationImpl.EMPTY,
          null,
          null);
    }
    RelNode x = convert(
        input,
        input.getTraitSet().replace(DrillRel.CONVENTION));
    call.transformTo(new DrillLimitRel(sort.getCluster(), traits, x, sort.offset, sort.fetch));
  }
}
