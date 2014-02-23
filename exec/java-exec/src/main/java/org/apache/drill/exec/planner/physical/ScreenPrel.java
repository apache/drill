package org.apache.drill.exec.planner.physical;

import java.util.List;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;

public class ScreenPrel extends SingleRel implements Prel{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScreenPrel.class);

  
  public ScreenPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child) {
    super(cluster, traits, child);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ScreenPrel(getCluster(), traitSet, sole(inputs));
  }
  
  
}
