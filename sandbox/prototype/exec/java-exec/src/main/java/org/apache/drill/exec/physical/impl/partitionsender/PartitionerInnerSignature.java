package org.apache.drill.exec.physical.impl.partitionsender;

import javax.inject.Named;

import org.apache.drill.exec.compile.sig.CodeGeneratorSignature;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

public interface PartitionerInnerSignature  extends CodeGeneratorSignature{
  
  public void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") OutgoingRecordBatch[] outgoing) throws SchemaChangeException;
  public void doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
  
  
  
}