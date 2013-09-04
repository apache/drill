package org.apache.drill.exec.record.selection;

import java.util.List;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;

import com.google.common.collect.Lists;

public class SelectionVector4Builder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SelectionVector4Builder.class);
  
  private List<BatchSchema> schemas = Lists.newArrayList();
  
  public void add(RecordBatch batch, boolean newSchema) throws SchemaChangeException{
    if(!schemas.isEmpty() && newSchema) throw new SchemaChangeException("Currently, the sv4 builder doesn't support embedded types");
    if(newSchema){
      schemas.add(batch.getSchema());
    }
    
  }
  
  
  // deals with managing selection vectors.
  // take a four byte int
  /**
   * take a four byte value
   * use the first two as a pointer.  use the other two as a
   * 
   *  we should manage an array of valuevectors
   */
  
  private class VectorSchemaBuilder{
    
  }
}
