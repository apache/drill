package org.apache.drill.exec.vector;

public class AllocationHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AllocationHelper.class);
  
  public static void allocate(ValueVector v, int valueCount, int bytesPerValue){
    if(v instanceof FixedWidthVector){
      ((FixedWidthVector) v).allocateNew(valueCount);
    }else if(v instanceof VariableWidthVector){
      ((VariableWidthVector) v).allocateNew(valueCount * bytesPerValue, valueCount);
    }else{
      throw new UnsupportedOperationException();
    }
  }
}
