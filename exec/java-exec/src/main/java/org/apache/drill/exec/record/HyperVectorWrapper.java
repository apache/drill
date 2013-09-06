package org.apache.drill.exec.record;

import java.lang.reflect.Array;

import org.apache.drill.exec.vector.ValueVector;


public class HyperVectorWrapper<T extends ValueVector> implements VectorWrapper<T>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HyperVectorWrapper.class);
  
  private T[] vectors;
  private MaterializedField f;
  private final boolean releasable;
  
  public HyperVectorWrapper(MaterializedField f, T[] v){
    this(f, v, true);
  }
  
  public HyperVectorWrapper(MaterializedField f, T[] v, boolean releasable){
    assert(v.length > 0);
    this.f = f;
    this.vectors = v;
    this.releasable = releasable;
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
  public void clear() {
    if(!releasable) return;
    for(T x : vectors){
      x.clear();  
    }
    
  }

  @Override
  @SuppressWarnings("unchecked")
  public VectorWrapper<T> cloneAndTransfer() {
    return new HyperVectorWrapper<T>(f, vectors, false);
//    T[] newVectors = (T[]) Array.newInstance(vectors.getClass().getComponentType(), vectors.length);
//    for(int i =0; i < newVectors.length; i++){
//      TransferPair tp = vectors[i].getTransferPair();
//      tp.transfer();
//      newVectors[i] = (T) tp.getTo();
//    }
//    return new HyperVectorWrapper<T>(f, newVectors);
  }

  public static <T extends ValueVector> HyperVectorWrapper<T> create(MaterializedField f, T[] v){
    return new HyperVectorWrapper<T>(f, v);
  }
}
