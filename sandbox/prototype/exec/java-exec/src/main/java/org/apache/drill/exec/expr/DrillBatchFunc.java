package org.apache.drill.exec.expr;

import org.apache.drill.exec.record.RecordBatch;

public interface DrillBatchFunc {
  public void setup(RecordBatch incoming);
  public void eval();
  public void batchReset();
  
}
