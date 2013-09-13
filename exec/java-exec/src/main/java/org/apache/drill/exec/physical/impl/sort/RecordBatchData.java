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

import java.util.List;

import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Lists;

/**
 * Holds the data for a particular record batch for later manipulation.
 */
public class RecordBatchData {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordBatchData.class);
  
  final List<ValueVector> vectors = Lists.newArrayList();
  final SelectionVector2 sv2;
  final int recordCount;
  VectorContainer container;
  
  public RecordBatchData(RecordBatch batch){
    this.sv2 = batch.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE ? batch.getSelectionVector2().clone() : null;
    
    for(VectorWrapper<?> v : batch){
      if(v.isHyper()) throw new UnsupportedOperationException("Record batch data can't be created based on a hyper batch.");
      TransferPair tp = v.getValueVector().getTransferPair();
      tp.transfer();
      vectors.add(tp.getTo());
    }
    
    recordCount = batch.getRecordCount();
  }
  
  public int getRecordCount(){
    return recordCount;
  }
  public List<ValueVector> getVectors() {
    return vectors;
  }

  public SelectionVector2 getSv2() {
    return sv2;
  }

  public VectorContainer getContainer() {
    if (this.container == null) buildContainer();
    return this.container;
  }

  private void buildContainer() {
    assert container == null;
    container = new VectorContainer();
    for (ValueVector vv : vectors) {
      container.add(vv);
    }
    container.buildSchema(sv2 == null ? SelectionVectorMode.NONE : SelectionVectorMode.TWO_BYTE);
  }
}
