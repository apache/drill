/**
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
 */
package org.apache.drill.exec.record;

import io.netty.buffer.ByteBuf;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos.FieldDef;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Maps;

public class RecordBatchLoader implements VectorAccessible, Iterable<VectorWrapper<?>>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordBatchLoader.class);

  private VectorContainer container = new VectorContainer();
  private final BufferAllocator allocator;
  private int valueCount;
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
    this.valueCount = def.getRecordCount();
    boolean schemaChanged = schema == null;

    Map<MaterializedField, ValueVector> oldFields = Maps.newHashMap();
    for(VectorWrapper<?> w : container){
      ValueVector v = w.getValueVector();
      oldFields.put(v.getField(), v);
    }
    
    VectorContainer newVectors = new VectorContainer();

    List<FieldMetadata> fields = def.getFieldList();

    int bufOffset = 0;
    for (FieldMetadata fmd : fields) {
      FieldDef fieldDef = fmd.getDef();
      ValueVector v = oldFields.remove(fieldDef);
      if(v != null){
        container.add(v);
        continue;
      }

      // if we arrive here, we didn't have a matching vector.
      schemaChanged = true;
      MaterializedField m = new MaterializedField(fieldDef);
      v = TypeHelper.getNewVector(m, allocator);
      if (fmd.getValueCount() == 0){
        v.clear();
      } else {
        v.load(fmd, buf.slice(bufOffset, fmd.getBufferLength()));
      }
      bufOffset += fmd.getBufferLength();
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
    for(VectorWrapper<?> v : newVectors){
      b.addField(v.getField());
    }
    b.setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
    this.schema = b.build();
    newVectors.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    container = newVectors;
    return schemaChanged;

  }

  public TypedFieldId getValueVectorId(SchemaPath path) {
    return container.getValueVectorId(path);
  }
  
  
  
//  
//  @SuppressWarnings("unchecked")
//  public <T extends ValueVector> T getValueVectorId(int fieldId, Class<?> clazz) {
//    ValueVector v = container.get(fieldId);
//    assert v != null;
//    if (v.getClass() != clazz){
//      logger.warn(String.format(
//          "Failure while reading vector.  Expected vector class of %s but was holding vector class %s.",
//          clazz.getCanonicalName(), v.getClass().getCanonicalName()));
//      return null;
//    }
//    return (T) v;
//  }

  public int getRecordCount() {
    return valueCount;
  }

  public VectorWrapper<?> getValueAccessorById(int fieldId, Class<?> clazz){
    return container.getValueAccessorById(fieldId, clazz);
  }
  
  public WritableBatch getWritableBatch(){
    return WritableBatch.getBatchNoSVWrap(valueCount, container);
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return this.container.iterator();
  }

  public BatchSchema getSchema(){
    return schema;
  }



}
