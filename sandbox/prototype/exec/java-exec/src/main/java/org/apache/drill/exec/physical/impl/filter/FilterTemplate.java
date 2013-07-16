package org.apache.drill.exec.physical.impl.filter;

import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;

public abstract class FilterTemplate {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterTemplate.class);
  
  SelectionVector2 outgoingSelectionVector;
  SelectionVector2 incomingSelectionVector;
  
  public void setup(RecordBatch incoming, RecordBatch outgoing){
    outgoingSelectionVector = outgoing.getSelectionVector2();

    switch(incoming.getSchema().getSelectionVector()){
    case NONE:
      break;
    case TWO_BYTE:
      this.incomingSelectionVector = incoming.getSelectionVector2();
      break;
    default:
      throw new UnsupportedOperationException();
    }
  }
  
  public void filterBatchSV2(int recordCount){
    int svIndex = 0;
    for(char i =0; i < recordCount; i++){
      if(include(i)){
        outgoingSelectionVector.setIndex(svIndex, i);
        svIndex+=2;
      }
    }
  }
  
  public void filterBatchNoSV(int recordCount){
    int svIndex = 0;
    for(char i =0; i < recordCount; i++){
      
      if(include(i)){
        outgoingSelectionVector.setIndex(svIndex, i);
        svIndex+=2;
      }
    }
  }
  
  protected abstract boolean include(int index);
}
