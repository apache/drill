package org.apache.drill.exec.physical.impl.svremover;

import javax.inject.Named;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

public abstract class CopierTemplate2 implements Copier{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CopierTemplate2.class);
  
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
    
    for(int svIndex = 0; svIndex < recordCount; svIndex++, outgoingPosition++){
      doEval(sv2.getIndex(svIndex), outgoingPosition);
    }
  }
  
  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing);
  public abstract void doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);

        

}
