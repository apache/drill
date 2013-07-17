package org.apache.drill.exec.physical.impl.filter;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

public interface FilterEvaluator {
  public void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing) throws SchemaChangeException;
  public boolean doEval(int inIndex, int outIndex);
}
