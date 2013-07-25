package org.apache.drill.exec.physical.impl.svremover;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

public interface CopyEvaluator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CopyEvaluator.class);
  
  public abstract void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing);
  public abstract void doEval(int incoming, int outgoing);
}
