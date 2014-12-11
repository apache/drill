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

import io.netty.buffer.DrillBuf;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Preconditions;
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
  public boolean load(RecordBatchDef def, DrillBuf buf) throws SchemaChangeException {
//    logger.debug("Loading record batch with def {} and data {}", def, buf);
    container.zeroVectors();
    this.valueCount = def.getRecordCount();
    boolean schemaChanged = schema == null;
//    logger.info("Load, ThreadID: {}", Thread.currentThread().getId(), new RuntimeException("For Stack Trace Only"));
//    System.out.println("Load, ThreadId: " + Thread.currentThread().getId());
    Map<MaterializedField, ValueVector> oldFields = Maps.newHashMap();
    for(VectorWrapper<?> w : container){
      ValueVector v = w.getValueVector();
      oldFields.put(v.getField(), v);
    }

    VectorContainer newVectors = new VectorContainer();

    List<SerializedField> fields = def.getFieldList();

    int bufOffset = 0;
    for (SerializedField fmd : fields) {
      MaterializedField fieldDef = MaterializedField.create(fmd);
      ValueVector vector = oldFields.remove(fieldDef);

      if (vector == null) {
        schemaChanged = true;
        vector = TypeHelper.getNewVector(fieldDef, allocator);
      } else if (!vector.getField().getType().equals(fieldDef.getType())) {
        // clear previous vector
        vector.clear();
        schemaChanged = true;
        vector = TypeHelper.getNewVector(fieldDef, allocator);
      }

      if (fmd.getValueCount() == 0 && (!fmd.hasGroupCount() || fmd.getGroupCount() == 0)) {
        AllocationHelper.allocate(vector, 0, 0, 0);
      } else {
        vector.load(fmd, buf.slice(bufOffset, fmd.getBufferLength()));
      }
      bufOffset += fmd.getBufferLength();
      newVectors.add(vector);
    }

    Preconditions.checkArgument(buf == null || bufOffset == buf.capacity());

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

  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids){
    return container.getValueAccessorById(clazz, ids);
  }

  public WritableBatch getWritableBatch(){
    boolean isSV2 = (schema.getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE);
    return WritableBatch.getBatchNoHVWrap(valueCount, container, isSV2);
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return this.container.iterator();
  }

  public BatchSchema getSchema(){
    return schema;
  }

  public void clear(){
    container.clear();
  }

  public void canonicalize() {
    //logger.debug( "RecordBatchLoader : before schema " + schema);
    container = VectorContainer.canonicalize(container);

    // rebuild the schema.
    SchemaBuilder b = BatchSchema.newBuilder();
    for(VectorWrapper<?> v : container){
      b.addField(v.getField());
    }
    b.setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
    this.schema = b.build();

    //logger.debug( "RecordBatchLoader : after schema " + schema);
  }
}
