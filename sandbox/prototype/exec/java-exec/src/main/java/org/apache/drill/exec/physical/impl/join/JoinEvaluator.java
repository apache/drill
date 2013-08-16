package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.exec.record.RecordBatch;

public interface JoinEvaluator {
  public abstract void setup(RecordBatch left, RecordBatch right, RecordBatch outgoing);
  public abstract boolean copy(int leftPosition, int rightPosition, int outputPosition);
  public abstract int compare(int leftPosition, int rightPosition);
}
