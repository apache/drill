/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.exec.record;

import io.netty.buffer.ByteBuf;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos.FieldDef;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.record.RecordBatch.TypedFieldId;
import org.apache.drill.exec.vector.TypeHelper;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class RecordBatchLoader implements Iterable<ValueVector>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordBatchLoader.class);

  private List<ValueVector> vectors = Lists.newArrayList();
  private final BufferAllocator allocator;
  private int recordCount; 
  private BatchSchema schema;
  
  public RecordBatchLoader(BufferAllocator allocator) {
    super();
    this.allocator = allocator;
  }

  /**
   * Load a record batch from a single buffer.
   * 
   * @param def
   *          The definition for the record batch.
   * @param buf
   *          The buffer that holds the data associated with the record batch
   * @return Whether or not the schema changed since the previous load.
   * @throws SchemaChangeException 
   */
  public boolean load(RecordBatchDef def, ByteBuf buf) throws SchemaChangeException {
//    logger.debug("Loading record batch with def {} and data {}", def, buf);
    this.recordCount = def.getRecordCount();
    boolean schemaChanged = false;

    Map<MaterializedField, ValueVector> oldFields = Maps.newHashMap();
    for(ValueVector v : this.vectors){
      oldFields.put(v.getField(), v);
    }
    
    List<ValueVector> newVectors = Lists.newArrayList();

    List<FieldMetadata> fields = def.getFieldList();
    
    int bufOffset = 0;
    for (FieldMetadata fmd : fields) {
      FieldDef fieldDef = fmd.getDef();
      ValueVector v = oldFields.remove(fieldDef);
      if(v != null){
        newVectors.add(v);
        continue;
      }
      
      // if we arrive here, we didn't have a matching vector.
      schemaChanged = true;
      MaterializedField m = new MaterializedField(fieldDef);
      v = TypeHelper.getNewVector(m, allocator);
      v.load(fmd, buf.slice(bufOffset, fmd.getBufferLength()));
      newVectors.add(v);
    }
    
    if(!oldFields.isEmpty()){
      schemaChanged = true;
      for(ValueVector v : oldFields.values()){
        v.close();
      }
    }
    
    // rebuild the schema.
    SchemaBuilder b = BatchSchema.newBuilder();
    for(ValueVector v : newVectors){
      b.addField(v.getField());
    }
    b.setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
    this.schema = b.build();
    vectors = ImmutableList.copyOf(newVectors);
    return schemaChanged;

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

  public int getRecordCount() {
    return recordCount;
  }


  public WritableBatch getWritableBatch(){
    return WritableBatch.getBatchNoSV(recordCount, vectors);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return this.vectors.iterator();
  }

  public BatchSchema getSchema(){
    return schema;
  }

  
  
}
