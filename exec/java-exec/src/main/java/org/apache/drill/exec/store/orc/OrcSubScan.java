package org.apache.drill.exec.store.orc;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.graph.GraphVisitor;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.physical.base.SubScan;

import java.util.Iterator;
import java.util.List;

public class OrcSubScan implements SubScan {
  public OrcSubScan() {

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
  public boolean isExecutable() {
    return false;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return null;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return null;
  }

  @Override
  public void accept(GraphVisitor<PhysicalOperator> visitor) {

  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return null;
  }
}
