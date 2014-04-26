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
package org.apache.drill.exec.planner.physical;

import java.io.IOException;

import org.apache.drill.exec.planner.common.DrillScanRelBase;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

public class ScanPrule extends RelOptRule{
  public static final RelOptRule INSTANCE = new ScanPrule();

  
  public ScanPrule() {
    super(RelOptHelper.any(DrillScanRelBase.class), "Prel.ScanPrule");
    
  }
  @Override
  public void onMatch(RelOptRuleCall call) {
    try{
      final DrillScanRelBase scan = (DrillScanRelBase) call.rel(0);
      DrillTable table = scan.getTable().unwrap(DrillTable.class);
      
      DrillDistributionTrait partition = table.getGroupScan().getMaxParallelizationWidth() > 1 ? DrillDistributionTrait.RANDOM_DISTRIBUTED : DrillDistributionTrait.SINGLETON;

//      DrillDistributionTrait partition = DrillDistributionTrait.SINGLETON;
//      
//      if (table instanceof DynamicDrillTable ) {
//        if (table.getGroupScan().getMaxParallelizationWidth() > 1 ) 
//          partition = DrillDistributionTrait.RANDOM_DISTRIBUTED;
//      }
      
      final RelTraitSet traits = scan.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(partition);

      DrillScanRelBase newScan = new ScanPrel(scan.getCluster(), traits, scan.getTable());
      call.transformTo(newScan);
      
    }catch(IOException e){
      throw new RuntimeException("Failure getting group scan.", e);
    }
  }

  
}
