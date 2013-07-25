package org.apache.drill.exec.physical.impl;

import java.util.Iterator;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.ScreenCreator.ScreenRoot;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.RecordBatch.TypedFieldId;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.ValueVector;

public class SimpleRootExec implements RootExec, Iterable<ValueVector>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleRootExec.class);

  private RecordBatch incoming;
  
  public SimpleRootExec(RootExec e){
    if(e instanceof ScreenRoot){
      incoming = ((ScreenRoot)e).getIncoming();  
    }else{
      throw new UnsupportedOperationException();
    }
    
  }

  public SelectionVector2 getSelectionVector2(){
    return incoming.getSelectionVector2();
  }

  public <T extends ValueVector> T getValueVectorById(SchemaPath path, Class<?> vvClass){
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

  @Override
  public Iterator<ValueVector> iterator() {
    return incoming.iterator();
  }

  public int getRecordCount(){
    return incoming.getRecordCount();
  }
  
  
}
