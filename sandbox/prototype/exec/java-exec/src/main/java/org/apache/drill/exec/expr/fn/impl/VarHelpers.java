package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.IntHolder;
import org.apache.drill.exec.vector.VarBinaryHolder;
import org.apache.drill.exec.vector.VarCharHolder;

public class VarHelpers {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VarHelpers.class);
  
  @FunctionTemplate(name = "compare_to", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class VarBinaryComparator implements DrillSimpleFunc {

      @Param VarBinaryHolder left;
      @Param VarBinaryHolder right;
      @Output IntHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        boolean doLengthEval = true;
        int i =0;
        for (int l = left.start, r = right.start; l < left.end && r < right.end; l++, r++, i++) {
          byte leftByte = left.buffer.getByte(l);
          byte rightByte = right.buffer.getByte(r);
          if (leftByte != rightByte) {
            out.value = ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
            doLengthEval = false;
            break;
          }
        }
        if(doLengthEval){
          int l = (left.end - left.start) - (right.end - right.start);
          if(l > 0){
            out.value = 1;
          }else if(l == 0){
            out.value = 0;
          }else{
            out.value = -1;
          }
        }

      }
     
  }
  
  @FunctionTemplate(name = "compare_to", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class VarCharComparator implements DrillSimpleFunc {

      @Param VarCharHolder left;
      @Param VarCharHolder right;
      @Output IntHolder out;

      public void setup(RecordBatch b) {}

      public void eval() {
        boolean doLengthEval = true;
        int i =0;
        for (int l = left.start, r = right.start; l < left.end && r < right.end; l++, r++, i++) {
          byte leftByte = left.buffer.getByte(l);
          byte rightByte = right.buffer.getByte(r);
          if (leftByte != rightByte) {
            out.value = ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
            doLengthEval = false;
            break;
          }
        }
        if(doLengthEval){
          int l = (left.end - left.start) - (right.end - right.start);
          if(l > 0){
            out.value = 1;
          }else if(l == 0){
            out.value = 0;
          }else{
            out.value = -1;
          }
        }

      }
     
  }  


      public static final int compare(VarBinaryHolder left, VarCharHolder right) {
        for (int l = left.start, r = right.start; l < left.end && r < right.end; l++, r++) {
          byte leftByte = left.buffer.getByte(l);
          byte rightByte = right.buffer.getByte(r);
          if (leftByte != rightByte) {
            return ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
          }
        }
        
        int l = (left.end - left.start) - (right.end - right.start);
        if(l > 0){
          return 1;
        }else if(l == 0){
          return 0;
        }else{
          return -1;
        }

      }
     
      public static final int compare(VarCharHolder left, VarCharHolder right) {
        for (int l = left.start, r = right.start; l < left.end && r < right.end; l++, r++) {
          byte leftByte = left.buffer.getByte(l);
          byte rightByte = right.buffer.getByte(r);
          if (leftByte != rightByte) {
            return ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
          }
        }
        
        int l = (left.end - left.start) - (right.end - right.start);
        if(l > 0){
          return 1;
        }else if(l == 0){
          return 0;
        }else{
          return -1;
        }

      }
      
      public static final int compare(VarBinaryHolder left, VarBinaryHolder right) {
        for (int l = left.start, r = right.start; l < left.end && r < right.end; l++, r++) {
          byte leftByte = left.buffer.getByte(l);
          byte rightByte = right.buffer.getByte(r);
          if (leftByte != rightByte) {
            return ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
          }
        }
        
        int l = (left.end - left.start) - (right.end - right.start);
        if(l > 0){
          return 1;
        }else if(l == 0){
          return 0;
        }else{
          return -1;
        }

      }
  
}
