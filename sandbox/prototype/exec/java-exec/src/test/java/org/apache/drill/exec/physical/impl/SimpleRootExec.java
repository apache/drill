package org.apache.drill.exec.physical.impl;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.ScreenCreator.ScreenRoot;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.RecordBatch.TypedFieldId;
import org.apache.drill.exec.record.vector.ValueVector;

public class SimpleRootExec implements RootExec{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleRootExec.class);

  private RecordBatch incoming;
  
  public SimpleRootExec(RootExec e){
    if(e instanceof ScreenRoot){
      incoming = ((ScreenRoot)e).getIncoming();  
    }else{
      throw new UnsupportedOperationException();
    }
    
  }


  public <T extends ValueVector<T>> T getValueVectorById(SchemaPath path, Class<?> vvClass){
    TypedFieldId tfid = incoming.getValueVectorId(path);
    return incoming.getValueVectorById(tfid.getFieldId(), vvClass);
  }
  
  @Override
  public boolean next() {
    return incoming.next() != IterOutcome.NONE;
  }

  @Override
  public void stop() {
  }

  
}
