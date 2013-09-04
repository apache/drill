package org.apache.drill.exec.expr;

import org.apache.drill.exec.record.RecordBatch;

public interface DrillAggFunc extends DrillFunc{
  public void setup(RecordBatch incoming);
  public void add();
  public void output();
  public void reset();
}
