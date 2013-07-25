package org.apache.drill.exec.vector;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;

abstract class BaseValueVector implements ValueVector{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseValueVector.class);
  
  protected final BufferAllocator allocator;
  protected final MaterializedField field;

  BaseValueVector(MaterializedField field, BufferAllocator allocator) {
    this.allocator = allocator;
    this.field = field;
  }
  
  @Override
  public void close() {
    clear();
  }
  
  @Override
  public MaterializedField getField() {
    return field;
  }
  
  abstract class BaseAccessor implements ValueVector.Accessor{
    public abstract int getValueCount();
    public void reset(){}
  }
  
  abstract class BaseMutator implements NonRepeatedMutator{
    public void reset(){}
  }
  
  
  
}

