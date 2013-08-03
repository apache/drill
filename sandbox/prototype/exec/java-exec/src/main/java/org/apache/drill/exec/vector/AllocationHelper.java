package org.apache.drill.exec.vector;

public class AllocationHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AllocationHelper.class);
  
  public static void allocate(ValueVector v, int valueCount, int bytesPerValue){
    allocate(v, valueCount, bytesPerValue, 5);
  }
  
  public static void allocate(ValueVector v, int valueCount, int bytesPerValue, int repeatedPerTop){
    if(v instanceof FixedWidthVector){
      ((FixedWidthVector) v).allocateNew(valueCount);
    } else if (v instanceof VariableWidthVector) {
      ((VariableWidthVector) v).allocateNew(valueCount * bytesPerValue, valueCount);
    }else if(v instanceof RepeatedFixedWidthVector){
      ((RepeatedFixedWidthVector) v).allocateNew(valueCount, valueCount * repeatedPerTop);
    }else if(v instanceof RepeatedVariableWidthVector){
      ((RepeatedVariableWidthVector) v).allocateNew(valueCount * bytesPerValue, valueCount, valueCount * repeatedPerTop);
    }else{
      throw new UnsupportedOperationException();
    }
  }
}
