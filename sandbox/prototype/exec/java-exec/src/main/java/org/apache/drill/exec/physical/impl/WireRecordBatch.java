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
package org.apache.drill.exec.physical.impl;

import java.util.Iterator;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.InvalidValueAccessor;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.record.RawFragmentBatchProvider;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.vector.ValueVector;

public class WireRecordBatch implements RecordBatch{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WireRecordBatch.class);

  private RecordBatchLoader batchLoader;
  private RawFragmentBatchProvider fragProvider;
  private FragmentContext context;
  private BatchSchema schema;

  
  public WireRecordBatch(FragmentContext context, RawFragmentBatchProvider fragProvider) {
    this.fragProvider = fragProvider;
    this.context = context;
    this.batchLoader = new RecordBatchLoader(context.getAllocator());
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  @Override
  public int getRecordCount() {
    return batchLoader.getRecordCount();
  }

  @Override
  public void kill() {
    fragProvider.kill(context);
  }

  @Override
  public <T extends ValueVector.Base> T getValueVector(int fieldId, Class<T> clazz) throws InvalidValueAccessor {
    return batchLoader.getValueVector(fieldId, clazz);
  }

  @Override
  public IterOutcome next() {
    RawFragmentBatch batch = fragProvider.getNext();
    try{
      if (batch == null) return IterOutcome.NONE;

      logger.debug("Next received batch {}", batch);

      RecordBatchDef rbd = batch.getHeader().getDef();
      boolean schemaChanged = batchLoader.load(rbd, batch.getBody());
      if(schemaChanged){
        this.schema = batchLoader.getSchema();
        return IterOutcome.OK_NEW_SCHEMA;
      }else{
        return IterOutcome.OK;
      }
    }catch(SchemaChangeException ex){
      context.fail(ex);
      return IterOutcome.STOP;
    }
  }

  @Override
  public WritableBatch getWritableBatch() {
    return batchLoader.getWritableBatch();
  }
  
  
}
