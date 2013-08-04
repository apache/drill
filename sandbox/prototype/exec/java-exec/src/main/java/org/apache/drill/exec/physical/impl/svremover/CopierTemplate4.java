package org.apache.drill.exec.physical.impl.svremover;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.svremover.RemovingRecordBatch.VectorAllocator;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

public abstract class CopierTemplate4 implements Copier{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CopierTemplate4.class);
  
  private SelectionVector4 sv4;
  private VectorAllocator[] allocators;

  
  private void allocateVectors(int recordCount){
    for(VectorAllocator a : allocators){
      a.alloc(recordCount);
    }
  }
  
  @Override
  public void setupRemover(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, VectorAllocator[] allocators) throws SchemaChangeException{
    this.allocators = allocators;
    this.sv4 = incoming.getSelectionVector4();
    doSetup(context, incoming, outgoing);
  }
  

  @Override
  public void copyRecords(){
    final int recordCount = sv4.getLength();
    allocateVectors(recordCount);
    int outgoingPosition = 0;
    final int end = sv4.getStart() + sv4.getLength();
    for(int svIndex = sv4.getStart(); svIndex < end; svIndex++, outgoingPosition++){
      int deRefIndex = sv4.get(svIndex);
      doEval(deRefIndex, outgoingPosition);
    }
  }
  
  public abstract void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing) throws SchemaChangeException;
  public abstract void doEval(int incoming, int outgoing);
        

}
