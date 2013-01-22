package org.apache.drill.exec.ref.rops;

import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.exceptions.SetupException;

public interface ROP {
  public void init(IteratorRegistry registry, EvaluatorFactory builder) throws SetupException;
  public RecordIterator getOutput();
  public void cleanup();
}
