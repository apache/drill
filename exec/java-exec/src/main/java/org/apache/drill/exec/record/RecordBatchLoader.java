/*
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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.StackTrace;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * Holds record batch loaded from record batch message.
 */
public class RecordBatchLoader implements VectorAccessible, Iterable<VectorWrapper<?>>{
  private final static Logger logger = LoggerFactory.getLogger(RecordBatchLoader.class);

  private final BufferAllocator allocator;
  private VectorContainer container = new VectorContainer();
  private int valueCount;
  private BatchSchema schema;

  /**
   * Constructs a loader using the given allocator for vector buffer allocation.
   */
  public RecordBatchLoader(BufferAllocator allocator) {
    this.allocator = Preconditions.checkNotNull(allocator);
  }

  public BufferAllocator allocator() { return allocator; }

  /**
   * Load a record batch from a single buffer.
   *
   * @param def
   *          The definition for the record batch.
   * @param buf
   *          The buffer that holds the data associated with the record batch.
   * @return Whether the schema changed since the previous load.
   * @throws SchemaChangeException
   *   TODO:  Clean:  DRILL-2933  load(...) never actually throws SchemaChangeException.
   */
  @SuppressWarnings("resource")
  public boolean load(RecordBatchDef def, DrillBuf buf) throws SchemaChangeException {
    if (logger.isTraceEnabled()) {
      logger.trace("Loading record batch with def {} and data {}", def, buf);
      logger.trace("Load, ThreadID: {}\n{}", Thread.currentThread().getId(), new StackTrace());
    }
    container.zeroVectors();
    valueCount = def.getRecordCount();
    boolean schemaChanged = schema == null;

    // Load vectors from the batch buffer, while tracking added and/or removed
    // vectors (relative to the previous call) in order to determine whether the
    // the schema has changed since the previous call.

    // Set up to recognize previous fields that no longer exist.
    final Map<String, ValueVector> oldFields = CaseInsensitiveMap.newHashMap();
    for (final VectorWrapper<?> wrapper : container) {
      final ValueVector vector = wrapper.getValueVector();
      oldFields.put(vector.getField().getName(), vector);
    }

    final VectorContainer newVectors = new VectorContainer();
    try {
      final List<SerializedField> fields = def.getFieldList();
      int bufOffset = 0;
      for (final SerializedField field : fields) {
        final MaterializedField fieldDef = MaterializedField.create(field);
        ValueVector vector = oldFields.remove(fieldDef.getName());

        if (vector == null) {
          // Field did not exist previously--is schema change.
          schemaChanged = true;
          vector = TypeHelper.getNewVector(fieldDef, allocator);
        } else if (! vector.getField().getType().equals(fieldDef.getType())) {
          // Field had different type before--is schema change.
          // clear previous vector
          vector.clear();
          schemaChanged = true;
          vector = TypeHelper.getNewVector(fieldDef, allocator);

        // If the field is a map, check if the map schema changed.

        } else if (vector.getField().getType().getMinorType() == MinorType.MAP  &&
                   ! isSameSchema(vector.getField().getChildren(), field.getChildList())) {

          // The map schema changed. Discard the old map and create a new one.

          schemaChanged = true;
          vector.clear();
          vector = TypeHelper.getNewVector(fieldDef, allocator);
        }

        // Load the vector.
        if (buf == null) {
          // Schema only
        } else if (field.getValueCount() == 0) {
          AllocationHelper.allocate(vector, 0, 0, 0);
        } else {
          vector.load(field, buf.slice(bufOffset, field.getBufferLength()));
        }
        bufOffset += field.getBufferLength();
        newVectors.add(vector);
      }

      // rebuild the schema.
      final SchemaBuilder builder = BatchSchema.newBuilder();
      for (final VectorWrapper<?> v : newVectors) {
        builder.addField(v.getField());
      }
      builder.setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
      schema = builder.build();
      newVectors.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      container = newVectors;
    } catch (final Throwable cause) {
      // We have to clean up new vectors created here and pass over the actual cause. It is upper layer who should
      // adjudicate to call upper layer specific clean up logic.
      for (final VectorWrapper<?> wrapper:newVectors) {
        wrapper.getValueVector().clear();
      }
      throw cause;
    } finally {
      if (! oldFields.isEmpty()) {
        schemaChanged = true;
        for (final ValueVector vector : oldFields.values()) {
          vector.clear();
        }
      }
    }

    return schemaChanged;
  }

  /**
   * Check if two schemas are the same. The schemas, given as lists, represent the
   * children of the original and new maps (AKA structures.)
   *
   * @param currentChildren current children of a Drill map
   * @param newChildren new children, in an incoming batch, of the same
   * Drill map
   * @return true if the schemas are identical, false if a child is missing
   * or has changed type or cardinality (AKA "mode").
   */

  private boolean isSameSchema(Collection<MaterializedField> currentChildren,
      List<SerializedField> newChildren) {
    if (currentChildren.size() != newChildren.size()) {
      return false;
    }

    // Column order can permute (see DRILL-5828). So, use a map
    // for matching.

    Map<String, MaterializedField> childMap = CaseInsensitiveMap.newHashMap();
    for (MaterializedField currentChild : currentChildren) {
      childMap.put(currentChild.getName(), currentChild);
    }
    for (SerializedField newChild : newChildren) {
      MaterializedField currentChild = childMap.get(newChild.getNamePart().getName());

      // New map member?

      if (currentChild == null) {
        return false;
      }

      // Changed data type?

      if (! currentChild.getType().equals(newChild.getMajorType())) {
        return false;
      }
    }

    // Everything matches.

    return true;
  }

  @Override
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

  @Override
  public int getRecordCount() { return valueCount; }

  public VectorContainer getContainer() { return container; }

  @Override
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

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BatchSchema getSchema() { return schema; }

  public void resetRecordCount() { valueCount = 0; }

  /**
   * Clears this loader, which clears the internal vector container (see
   * {@link VectorContainer#clear}) and resets the record count to zero.
   */
  public void clear() {
    container.clear();
    resetRecordCount();
  }
}
