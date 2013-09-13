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
package org.apache.drill.exec.physical.impl.sort;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.BufferAllocator.PreAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.ArrayListMultimap;

public class SortRecordBatchBuilder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SortRecordBatchBuilder.class);
  
  private final ArrayListMultimap<BatchSchema, RecordBatchData> batches = ArrayListMultimap.create();
  private final VectorContainer container;

  private int recordCount;
  private long runningBytes;
  private long runningBatches;
  private final long maxBytes;
  private SelectionVector4 sv4;
  final PreAllocator svAllocator;
  
  public SortRecordBatchBuilder(BufferAllocator a, long maxBytes, VectorContainer container){
    this.maxBytes = maxBytes;
    this.svAllocator = a.getPreAllocator();
    this.container = container;
  }
  
  private long getSize(RecordBatch batch){
    long bytes = 0;
    for(VectorWrapper<?> v : batch){
      bytes += v.getValueVector().getBufferSize();
    }
    return bytes;
  }
  
  /**
   * Add another record batch to the set of record batches.  
   * @param batch
   * @return True if the requested add completed successfully.  Returns false in the case that this builder is full and cannot receive additional packages. 
   * @throws SchemaChangeException
   */
  public boolean add(RecordBatch batch){
    if(batch.getSchema().getSelectionVectorMode() == SelectionVectorMode.FOUR_BYTE) throw new UnsupportedOperationException("A sort cannot currently work against a sv4 batch.");
    if (batch.getRecordCount() == 0) return true; // skip over empty record batches.

    long batchBytes = getSize(batch);
    if (batchBytes == 0) {return true;}
    if(batchBytes + runningBytes > maxBytes) return false; // enough data memory.
    if(runningBatches+1 > Character.MAX_VALUE) return false; // allowed in batch.
    if(!svAllocator.preAllocate(batch.getRecordCount()*4)) return false;  // sv allocation available.
      

    if (batch.getRecordCount() == 0) return true;
    RecordBatchData bd = new RecordBatchData(batch);
    runningBytes += batchBytes;
    batches.put(batch.getSchema(), bd);
    recordCount += bd.getRecordCount();
    return true;
  }

  public void build(FragmentContext context) throws SchemaChangeException{
    container.clear();
    if(batches.keySet().size() > 1) throw new SchemaChangeException("Sort currently only supports a single schema.");
    if(batches.size() > Character.MAX_VALUE) throw new SchemaChangeException("Sort cannot work on more than %d batches at a time.", (int) Character.MAX_VALUE);
    sv4 = new SelectionVector4(svAllocator.getAllocation(), recordCount, Character.MAX_VALUE);
    BatchSchema schema = batches.keySet().iterator().next();
    List<RecordBatchData> data = batches.get(schema);
    
    // now we're going to generate the sv4 pointers
    switch(schema.getSelectionVectorMode()){
    case NONE: {
      int index = 0;
      int recordBatchId = 0;
      for(RecordBatchData d : data){
        for(int i =0; i < d.getRecordCount(); i++, index++){
          sv4.set(index, recordBatchId, i);
        }
        recordBatchId++;
      }
      break;
    }
    case TWO_BYTE: {
      int index = 0;
      int recordBatchId = 0;
      for(RecordBatchData d : data){
        for(int i =0; i < d.getRecordCount(); i++, index++){
          sv4.set(index, recordBatchId, (int) d.getSv2().getIndex(i));
        }
        // might as well drop the selection vector since we'll stop using it now.
        d.getSv2().clear();
        recordBatchId++;
      }
      break;
    }
    default:
      throw new UnsupportedOperationException();
    }
    
    // next, we'll create lists of each of the vector types.
    ArrayListMultimap<MaterializedField, ValueVector> vectors = ArrayListMultimap.create();
    for(RecordBatchData rbd : batches.values()){
      for(ValueVector v : rbd.vectors){
        vectors.put(v.getField(), v);
      }
    }
    
    for(MaterializedField f : vectors.keySet()){
      List<ValueVector> v = vectors.get(f);
      container.addHyperList(v);
    }
    
    container.buildSchema(SelectionVectorMode.FOUR_BYTE);
  }

  public SelectionVector4 getSv4() {
    return sv4;
  }

  public List<VectorContainer> getContainers() {
    ArrayList containerList = Lists.newArrayList();
    int recordCount = 0;
    for (BatchSchema bs : batches.keySet()) {
      for (RecordBatchData bd : batches.get(bs)) {
        VectorContainer c = bd.getContainer();
        c.setRecordCount(bd.getRecordCount());
        containerList.add(c);
      }
    }
    return containerList;
  }
  
}
