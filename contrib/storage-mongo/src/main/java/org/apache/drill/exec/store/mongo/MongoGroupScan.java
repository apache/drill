package org.apache.drill.exec.store.mongo;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

public class MongoGroupScan extends AbstractGroupScan implements
    DrillMongoConstants {

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints)
      throws PhysicalOperatorSetupException {

  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId)
      throws ExecutionSetupException {
    return null;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 0;
  }

  @Override
  public String getDigest() {
    return null;
  }

  @Override
  public ScanStats getScanStats() {
    return null;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    return null;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return null;
  }

}
