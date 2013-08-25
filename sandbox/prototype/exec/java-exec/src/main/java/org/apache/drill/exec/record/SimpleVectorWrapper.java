package org.apache.drill.exec.record;

import org.apache.drill.exec.vector.ValueVector;

public class SimpleVectorWrapper<T extends ValueVector> implements VectorWrapper<T>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleVectorWrapper.class);
  
  private T v;

  public SimpleVectorWrapper(T v){
    this.v = v;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<T> getVectorClass() {
    return (Class<T>) v.getClass();
  }

  @Override
  public MaterializedField getField() {
    return v.getField();
  }

  @Override
  public T getValueVector() {
    return v;
  }

  @Override
  public T[] getValueVectors() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isHyper() {
    return false;
  }
  
  
  @SuppressWarnings("unchecked")
  @Override
  public VectorWrapper<T> cloneAndTransfer() {
    TransferPair tp = v.getTransferPair();
    tp.transfer();
    return new SimpleVectorWrapper<T>((T) tp.getTo());
  }

  @Override
  public void release() {
    v.clear();
  }

  public static <T extends ValueVector> SimpleVectorWrapper<T> create(T v){
    return new SimpleVectorWrapper<T>(v);
  }
}
