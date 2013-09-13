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
package org.apache.drill.exec.physical.impl.orderedpartitioner;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.BufferAllocator.PreAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import java.util.ArrayList;
import java.util.List;

public class SortContainerBuilder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SortContainerBuilder.class);

  private final ArrayListMultimap<BatchSchema, RecordBatchData> batches = ArrayListMultimap.create();
  private final VectorContainer container;

  private int recordCount;
  private SelectionVector4 sv4;
  final PreAllocator svAllocator;
  private List<List<ValueVector>> hyperVectors;

  public SortContainerBuilder(BufferAllocator a, long maxBytes, VectorContainer container, int fields){
    this.svAllocator = a.getPreAllocator();
    this.container = container;
    this.hyperVectors = Lists.newArrayList();
    for (int i = 0; i < fields; i++) {
      List<ValueVector> list = Lists.newArrayList();
      hyperVectors.add(list);
    }
  }

  private long getSize(RecordBatch batch){
    long bytes = 0;
    for(VectorWrapper<?> v : batch){
      bytes += v.getValueVector().getBufferSize();
    }
    return bytes;
  }

  public boolean add(List<ValueVector> vectors){
    Preconditions.checkArgument(vectors.size() == hyperVectors.size());
    int i = 0;
    for (ValueVector vv : vectors) {
      if (i == 0) recordCount += vv.getMetadata().getValueCount();
      hyperVectors.get(i++).add(vv);
    }
    return true;
  }

  public void build(FragmentContext context) throws SchemaChangeException{
    container.clear();
    svAllocator.preAllocate(recordCount * 4);
    sv4 = new SelectionVector4(svAllocator.getAllocation(), recordCount, Character.MAX_VALUE);

    // now we're going to generate the sv4 pointers
    int index = 0;
    int recordBatchId = 0;
    for(ValueVector vv : hyperVectors.get(0)){
      for(int i = 0; i < vv.getMetadata().getValueCount(); i++, index++){
        sv4.set(index, recordBatchId, i);
      }
      recordBatchId++;
    }

    for (List<ValueVector> hyperVector : hyperVectors) {
      container.addHyperList(hyperVector);
    }

    container.buildSchema(SelectionVectorMode.FOUR_BYTE);
  }

  public SelectionVector4 getSv4() {
    return sv4;
  }
  
}
