package org.apache.drill.exec.record;

import org.apache.drill.exec.vector.ValueVector;


public interface VectorWrapper<T extends ValueVector> {

  public Class<T> getVectorClass();
  public MaterializedField getField();
  public T getValueVector();
  public T[] getValueVectors();
  public boolean isHyper();
  public void release();
  public VectorWrapper<T> cloneAndTransfer();
}
