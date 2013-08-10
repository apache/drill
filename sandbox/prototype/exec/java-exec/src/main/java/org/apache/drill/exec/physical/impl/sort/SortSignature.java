package org.apache.drill.exec.physical.impl.sort;

import javax.inject.Named;

import org.apache.drill.exec.compile.sig.CodeGeneratorSignature;
import org.apache.drill.exec.compile.sig.DefaultGeneratorSignature;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;

public interface SortSignature extends CodeGeneratorSignature{
  
  public static final MappingSet MAIN_MAPPING = new MappingSet("null", "null", DefaultGeneratorSignature.DEFAULT_SCALAR_MAP, DefaultGeneratorSignature.DEFAULT_SCALAR_MAP);
  public static final MappingSet LEFT_MAPPING = new MappingSet("leftIndex", "null", DefaultGeneratorSignature.DEFAULT_SCALAR_MAP, DefaultGeneratorSignature.DEFAULT_SCALAR_MAP);
  public static final MappingSet RIGHT_MAPPING = new MappingSet("rightIndex", "null", DefaultGeneratorSignature.DEFAULT_SCALAR_MAP, DefaultGeneratorSignature.DEFAULT_SCALAR_MAP);

  public void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing);
  public int doEval(@Named("leftIndex") int leftIndex, @Named("rightIndex") int rightIndex);
  
}
