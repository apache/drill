package org.apache.drill.exec.vector.allocator;

import org.apache.drill.exec.vector.FixedWidthVector;

public class FixedVectorAllocator extends VectorAllocator{
  FixedWidthVector out;
  
  public FixedVectorAllocator(FixedWidthVector out) {
    super();
    this.out = out;
  }

  public void alloc(int recordCount){
    out.allocateNew(recordCount);
  }

  @Override
  public String toString() {
    return "FixedVectorAllocator [out=" + out + "]";
  }
  
  
}