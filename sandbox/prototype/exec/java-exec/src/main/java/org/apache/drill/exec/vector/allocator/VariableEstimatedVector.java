package org.apache.drill.exec.vector.allocator;

import org.apache.drill.exec.vector.VariableWidthVector;

public class VariableEstimatedVector extends VectorAllocator{
  VariableWidthVector out;
  int avgWidth;
  
  public VariableEstimatedVector(VariableWidthVector out, int avgWidth) {
    super();
    this.out = out;
    this.avgWidth = avgWidth;
  }
  
  public void alloc(int recordCount){
    out.allocateNew(avgWidth * recordCount, recordCount);
    out.getMutator().setValueCount(recordCount);
  }
}