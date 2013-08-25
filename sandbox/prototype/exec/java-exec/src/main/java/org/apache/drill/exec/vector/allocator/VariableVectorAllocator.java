package org.apache.drill.exec.vector.allocator;

import org.apache.drill.exec.vector.VariableWidthVector;

class VariableVectorAllocator extends VectorAllocator{
  VariableWidthVector in;
  VariableWidthVector out;
  
  public VariableVectorAllocator(VariableWidthVector in, VariableWidthVector out) {
    super();
    this.in = in;
    this.out = out;
  }

  public void alloc(int recordCount){
    out.allocateNew(in.getByteCapacity(), recordCount);
    out.getMutator().setValueCount(recordCount);
  }
}