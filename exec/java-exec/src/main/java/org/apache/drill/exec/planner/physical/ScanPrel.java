package org.apache.drill.exec.planner.physical;

import java.io.IOException;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.common.BaseScanRel;
import org.apache.drill.exec.store.StoragePlugin;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

public class ScanPrel extends BaseScanRel implements Prel{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanPrel.class);

  public ScanPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable tbl) {
    super(DRILL_PHYSICAL, cluster, traits, tbl);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    StoragePlugin plugin = this.drillTable.getPlugin();
    GroupScan scan = plugin.getPhysicalScan(new JSONOptions(drillTable.getSelection()));
    return scan;    
  }
  
  
}
