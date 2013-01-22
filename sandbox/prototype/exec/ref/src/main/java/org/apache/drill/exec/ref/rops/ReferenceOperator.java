package org.apache.drill.exec.ref.rops;

import org.apache.drill.exec.ref.RecordIterator;

public interface ReferenceOperator {
  
  public void setInput(RecordIterator incoming);
  public RecordIterator getOutput();
}
