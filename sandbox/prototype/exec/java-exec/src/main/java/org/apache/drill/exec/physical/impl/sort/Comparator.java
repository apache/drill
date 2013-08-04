package org.apache.drill.exec.physical.impl.sort;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

public interface Comparator {
  
  public abstract void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing) throws SchemaChangeException;
  public abstract int doEval(int inIndex, int outIndex);
}
