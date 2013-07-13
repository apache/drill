package org.apache.drill.exec.store.orc;

import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;

import java.util.List;

@JsonTypeName("orc-scan")
public class OrcGroupScan extends AbstractGroupScan {
  private OrcStorageEngine storageEngine;
  private OrcStorageEngineConfig engineConfig;

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {

  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    return null;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 0;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return null;
  }

  @Override
  public OperatorCost getCost() {
    return null;
  }

  @Override
  public Size getSize() {
    return null;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return null;
  }
}
