package org.apache.drill.exec.store.mongo;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;

public class MongoSubScan extends AbstractBase implements SubScan{

  @Override
  public <T, X, E extends Throwable> T accept(
      PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return null;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    return null;
  }

  @Override
  public int getOperatorType() {
    return 0;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return null;
  }

}
