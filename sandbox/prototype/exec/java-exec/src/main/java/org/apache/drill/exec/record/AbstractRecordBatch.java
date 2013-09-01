package org.apache.drill.exec.record;

import java.util.Iterator;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

public abstract class AbstractRecordBatch<T extends PhysicalOperator> implements RecordBatch{
  final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(this.getClass());
  
  protected final VectorContainer container = new VectorContainer();
  protected final T popConfig;
  protected final FragmentContext context;
  
  protected AbstractRecordBatch(T popConfig, FragmentContext context) {
    super();
    this.context = context;
    this.popConfig = popConfig;
  }
  
  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return container.iterator();
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  public PhysicalOperator getPopConfig() {
    return popConfig;
  }

  @Override
  public BatchSchema getSchema() {
    return container.getSchema();
  }

  @Override
  public void kill() {
    container.clear();
    killIncoming();
    cleanup();
  }
  
  protected abstract void killIncoming();
  
  protected void cleanup(){
  }
  
  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return container.getValueVector(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(int fieldId, Class<?> clazz) {
    return container.getValueAccessorById(fieldId, clazz);
  }

  
  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }
  
  
}
