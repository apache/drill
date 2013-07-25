package org.apache.drill.exec.vector;

import org.apache.drill.exec.vector.ValueVector.Mutator;

public interface NonRepeatedMutator extends Mutator{
  public void setValueCount(int valueCount);
}
