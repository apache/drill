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

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos.FieldDef;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.record.vector.TypeHelper;
import org.apache.drill.exec.record.vector.ValueVector;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;

public class RecordBatchLoader implements Iterable<IntObjectCursor<ValueVector.Base>>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordBatchLoader.class);

  private IntObjectOpenHashMap<ValueVector.Base> vectors = new IntObjectOpenHashMap<ValueVector.Base>();
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
   *          The buffer that holds the data ssociated with the record batch
   * @return Whether or not the schema changed since the previous load.
   * @throws SchemaChangeException 
   */
  public boolean load(RecordBatchDef def, ByteBuf buf) throws SchemaChangeException {
//    logger.debug("Loading record batch with def {} and data {}", def, buf);
    this.recordCount = def.getRecordCount();
    boolean schemaChanged = false;
    
    IntObjectOpenHashMap<ValueVector.Base> newVectors = new IntObjectOpenHashMap<ValueVector.Base>();

    List<FieldMetadata> fields = def.getFieldList();
    
    int bufOffset = 0;
    for (FieldMetadata fmd : fields) {
      FieldDef fieldDef = fmd.getDef();
      ValueVector.Base v = vectors.remove(fieldDef.getFieldId());
      if (v != null) {
        if (v.getField().getDef().equals(fieldDef)) {
          v.allocateNew(fmd.getBufferLength(), buf.slice(bufOffset, fmd.getBufferLength()), recordCount);
          newVectors.put(fieldDef.getFieldId(), v);
          continue;
        } else {
          v.close();
          v = null;
        }
      }
      // if we arrive here, either the metadata didn't match, or we didn't find a vector.
      schemaChanged = true;
      MaterializedField m = new MaterializedField(fieldDef);
      v = TypeHelper.getNewVector(m, allocator);
      v.allocateNew(fmd.getBufferLength(), buf.slice(bufOffset, fmd.getBufferLength()), recordCount);
      newVectors.put(fieldDef.getFieldId(), v);
    }
    
    if(!vectors.isEmpty()){
      schemaChanged = true;
      for(IntObjectCursor<ValueVector.Base> cursor : newVectors){
        cursor.value.close();
      }
      
    }
    
    if(schemaChanged){
      // rebuild the schema.
      SchemaBuilder b = BatchSchema.newBuilder();
      for(IntObjectCursor<ValueVector.Base> cursor : newVectors){
        b.addField(cursor.value.getField());
      }
      b.setSelectionVector(false);
      this.schema = b.build();
    }
    vectors = newVectors;
    return schemaChanged;

  }

  @SuppressWarnings("unchecked")
  public <T extends ValueVector.Base> T getValueVector(int fieldId, Class<T> clazz) throws InvalidValueAccessor {
    ValueVector.Base v = vectors.get(fieldId);
    assert v != null;
    if (v.getClass() != clazz)
      throw new InvalidValueAccessor(String.format(
          "Failure while reading vector.  Expected vector class of %s but was holding vector class %s.",
          clazz.getCanonicalName(), v.getClass().getCanonicalName()));
    return (T) v;
  }

  public int getRecordCount() {
    return recordCount;
  }


  public WritableBatch getWritableBatch(){
    return WritableBatch.get(recordCount, vectors);
  }

  @Override
  public Iterator<IntObjectCursor<ValueVector.Base>> iterator() {
    return this.vectors.iterator();
  }

  public BatchSchema getSchema(){
    return schema;
  }

  
  
}
