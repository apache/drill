package org.apache.drill.exec.ref;

import org.apache.drill.exec.ref.rops.ReferenceOperator;

public class ReferenceTransform implements ReferenceOperator{

  @Override
  public void setInput(RecordIterator incoming) {
  }

  @Override
  public RecordIterator getOutput() {
    return null;
  }

  
}
