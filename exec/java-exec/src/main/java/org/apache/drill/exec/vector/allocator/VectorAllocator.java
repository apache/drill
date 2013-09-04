package org.apache.drill.exec.vector.allocator;

import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;

public abstract class VectorAllocator{
  public abstract void alloc(int recordCount);
  
  public static VectorAllocator getAllocator(ValueVector in, ValueVector outgoing){
    if(outgoing instanceof FixedWidthVector){
      return new FixedVectorAllocator((FixedWidthVector) outgoing);
    }else if(outgoing instanceof VariableWidthVector && in instanceof VariableWidthVector){
      return new VariableVectorAllocator( (VariableWidthVector) in, (VariableWidthVector) outgoing);
    }else{
      throw new UnsupportedOperationException();
    }
  }
    
  
  public static VectorAllocator getAllocator(ValueVector outgoing, int averageBytesPerVariable){
    if(outgoing instanceof FixedWidthVector){
      return new FixedVectorAllocator((FixedWidthVector) outgoing);
    }else if(outgoing instanceof VariableWidthVector){
      return new VariableEstimatedVector( (VariableWidthVector) outgoing, averageBytesPerVariable);
    }else{
      throw new UnsupportedOperationException();
    }
  }
}