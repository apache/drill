package org.apache.drill.exec.record;

import org.apache.drill.exec.vector.ValueVector;

public interface TransferPair {
  public void transfer();
  public ValueVector getTo();
}
