package org.apache.drill.exec.physical.impl.project;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

public interface ProjectEvaluator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectEvaluator.class);
  
  public abstract void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing) throws SchemaChangeException;
  public abstract void doEval(int inIndex, int outIndex);
}
