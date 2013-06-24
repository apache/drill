package org.apache.drill.exec.expr;

import org.apache.drill.exec.record.RecordBatch;

public interface DrillAggrFunc {
  public void setup(RecordBatch incoming);
  public void add();
  public void eval();
}
