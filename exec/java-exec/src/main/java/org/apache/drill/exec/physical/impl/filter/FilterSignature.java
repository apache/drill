package org.apache.drill.exec.physical.impl.filter;

import javax.inject.Named;

import org.apache.drill.exec.compile.sig.CodeGeneratorSignature;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

public interface FilterSignature  extends CodeGeneratorSignature{
  
  public void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing);
  public boolean doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
  
}
