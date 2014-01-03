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
package org.apache.drill.exec.physical.impl.svremover;

import javax.inject.Named;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

public abstract class CopierTemplate4 implements Copier{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CopierTemplate4.class);
  
  private SelectionVector4 sv4;
  private VectorAllocator[] allocators;
  private RecordBatch incoming;
  
  private void allocateVectors(int recordCount){
    for(VectorAllocator a : allocators){
      a.alloc(recordCount);
    }
  }
  
  @Override
  public void setupRemover(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, VectorAllocator[] allocators) throws SchemaChangeException{
    this.allocators = allocators;
    this.incoming = incoming;
    this.sv4 = incoming.getSelectionVector4();
    doSetup(context, incoming, outgoing);
  }
  

  @Override
  public void copyRecords(){
//    logger.debug("Copying records.");
    final int recordCount = sv4.getCount();
    allocateVectors(recordCount);
    int outgoingPosition = 0;
    for(int svIndex = 0; svIndex < sv4.getCount(); svIndex++, outgoingPosition++){
      int deRefIndex = sv4.get(svIndex);
      doEval(deRefIndex, outgoingPosition);
    }
    
    for(VectorWrapper<?> v : incoming){
      v.clear();
    }

  }
  
  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing);
  public abstract void doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);

        

}
