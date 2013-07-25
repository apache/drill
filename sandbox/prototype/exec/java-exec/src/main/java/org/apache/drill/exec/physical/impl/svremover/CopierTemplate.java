package org.apache.drill.exec.physical.impl.svremover;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.svremover.RemovingRecordBatch.VectorAllocator;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;

public abstract class CopierTemplate implements Copier{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CopierTemplate.class);
  
  private SelectionVector2 sv2;
  private VectorAllocator[] allocators;
  
  @Override
  public void setupRemover(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, VectorAllocator[] allocators) throws SchemaChangeException{
    this.allocators = allocators;
    this.sv2 = incoming.getSelectionVector2();
    doSetup(context, incoming, outgoing);
  }
  
  private void allocateVectors(int recordCount){
    for(VectorAllocator a : allocators){
      a.alloc(recordCount);
    }
  }
  
  @Override
  public void copyRecords(){
    final int recordCount = sv2.getCount();
    allocateVectors(recordCount);
    int outgoingPosition = 0;
    
    for(int svIndex = 0; svIndex < recordCount * 2; svIndex++, outgoingPosition++){
      doEval(svIndex, outgoingPosition);
    }
  }
  
  public abstract void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing) throws SchemaChangeException;
  public abstract void doEval(int incoming, int outgoing);
        

}
