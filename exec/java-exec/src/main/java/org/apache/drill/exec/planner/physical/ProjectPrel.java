package org.apache.drill.exec.planner.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.planner.common.BaseProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

public class ProjectPrel extends BaseProjectRel implements Prel{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectPrel.class);
  
  
  protected ProjectPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexNode> exps,
      RelDataType rowType) {
    super(DRILL_PHYSICAL, cluster, traits, child, exps, rowType);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ProjectPrel(getCluster(), traitSet, sole(inputs), new ArrayList<RexNode>(exps), rowType);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();
    Project p = new Project(this.getProjectExpressions(creator.getContext()), child.getPhysicalOperator(creator));
    return p;
  }


}
