package org.apache.drill.exec.compile.sig;

import static org.apache.drill.exec.compile.sig.GeneratorMapping.GM;

import javax.inject.Named;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

public interface DefaultGeneratorSignature extends CodeGeneratorSignature{
  
  public static final GeneratorMapping DEFAULT_SCALAR_MAP = GM("doSetup", "doEval", null, null);
  public static final GeneratorMapping DEFAULT_CONSTANT_MAP = GM("doSetup", "doSetup", null, null);
  
  public static final MappingSet DEFAULT_MAPPING = new MappingSet("inIndex", "outIndex", DEFAULT_SCALAR_MAP, DEFAULT_SCALAR_MAP);

  public void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing);
  public void doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
  
  
  
}
