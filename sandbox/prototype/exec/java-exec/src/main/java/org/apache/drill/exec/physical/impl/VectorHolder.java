package org.apache.drill.exec.physical.impl;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.RecordBatch.TypedFieldId;
import org.apache.drill.exec.vector.ValueVector;

public class VectorHolder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorHolder.class);
  
  private List<ValueVector> vectors;

  public VectorHolder(List<ValueVector> vectors) {
    super();
    this.vectors = vectors;
  }
  
  public TypedFieldId getValueVector(SchemaPath path) {
    for(int i =0; i < vectors.size(); i++){
      ValueVector vv = vectors.get(i);
      if(vv.getField().matches(path)) return new TypedFieldId(vv.getField().getType(), i); 
    }
    return null;
  }
  
  @SuppressWarnings("unchecked")
  public <T extends ValueVector> T getValueVector(int fieldId, Class<?> clazz) {
    ValueVector v = vectors.get(fieldId);
    assert v != null;
    if (v.getClass() != clazz){
      logger.warn(String.format(
          "Failure while reading vector.  Expected vector class of %s but was holding vector class %s.",
          clazz.getCanonicalName(), v.getClass().getCanonicalName()));
      return null;
    }
    return (T) v;
  }
}
