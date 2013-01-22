package org.apache.drill.exec.ref.util;

import org.apache.drill.exec.ref.RecordPointer;

public interface BatchBreaker {
  public boolean shouldBreakAfter(RecordPointer record);
  
  public static class CountBreaker implements BatchBreaker{
    private final int max;
    private int count = 0;
    
    public CountBreaker(int max) {
      super();
      this.max = max;
    }

    @Override
    public boolean shouldBreakAfter(RecordPointer record) {
      count++;
      if(count > max){
        count = 0;
        return true;
      }{
        return false;
      }
    }
    
  }
}
