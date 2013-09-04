package org.apache.drill.exec.physical.impl.aggregate;

import org.apache.drill.exec.record.RecordBatch.IterOutcome;

public interface BatchIterator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchIterator.class);
  
  public IterOutcome next();
}
