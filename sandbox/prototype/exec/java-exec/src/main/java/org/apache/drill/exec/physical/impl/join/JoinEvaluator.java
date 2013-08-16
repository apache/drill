package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.VectorContainer;

public interface JoinEvaluator {
  public abstract void doSetup(FragmentContext context, JoinStatus status, VectorContainer outgoing) throws SchemaChangeException;

}
