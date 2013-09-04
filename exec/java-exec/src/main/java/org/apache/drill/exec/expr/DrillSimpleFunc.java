package org.apache.drill.exec.expr;

import org.apache.drill.exec.record.RecordBatch;

public interface DrillSimpleFunc extends DrillFunc{
  public void setup(RecordBatch incoming);
  public void eval();
}
