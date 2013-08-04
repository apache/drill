package org.apache.drill.exec.record;

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
  
  public static <T extends ValueVector> HyperVectorWrapper<T> create(MaterializedField f, T[] v){
    return new HyperVectorWrapper<T>(f, v);
  }
}
