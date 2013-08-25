package org.apache.drill.exec.record;

import java.lang.reflect.Array;

import org.apache.drill.exec.vector.ValueVector;


public class HyperVectorWrapper<T extends ValueVector> implements VectorWrapper<T>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HyperVectorWrapper.class);
  
  private T[] vectors;
  private MaterializedField f;

  public HyperVectorWrapper(MaterializedField f, T[] v){
    assert(v.length > 0);
    this.f = f;
    this.vectors = v;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<T> getVectorClass() {
    return (Class<T>) vectors.getClass().getComponentType();
  }

  @Override
  public MaterializedField getField() {
    return f;
  }

  @Override
  public T getValueVector() {
    throw new UnsupportedOperationException();
  }

  @Override
  public T[] getValueVectors() {
    return vectors;
  }

  @Override
  public boolean isHyper() {
    return true;
  }

  @Override
  public void release() {
    for(T x : vectors){
      x.clear();  
    }
    
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public VectorWrapper<T> cloneAndTransfer() {
    T[] newVectors = (T[]) Array.newInstance(vectors.getClass().getComponentType(), vectors.length);
    for(int i =0; i < newVectors.length; i++){
      TransferPair tp = vectors[i].getTransferPair();
      tp.transfer();
      newVectors[i] = (T) tp.getTo();
    }
    return new HyperVectorWrapper<T>(f, newVectors);
  }

  public static <T extends ValueVector> HyperVectorWrapper<T> create(MaterializedField f, T[] v){
    return new HyperVectorWrapper<T>(f, v);
  }
}
