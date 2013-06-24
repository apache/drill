package org.apache.drill.exec.expr;

import org.apache.drill.exec.record.RecordBatch;

public interface DrillFunc {
  public void setup(RecordBatch incoming);
  public void eval();
  
}
