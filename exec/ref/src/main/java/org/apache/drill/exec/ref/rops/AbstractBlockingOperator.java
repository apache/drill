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
package org.apache.drill.exec.ref.rops;

import org.apache.drill.common.logical.data.SingleInputOperator;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.RecordIterator.NextOutcome;


public abstract class AbstractBlockingOperator<T extends SingleInputOperator> extends SingleInputROPBase<T> {

  public AbstractBlockingOperator(T config) {
    super(config);
  }

  private RecordIterator incoming;
  protected RecordPointer inputRecord;
  protected final ProxySimpleRecord outputRecord = new ProxySimpleRecord();

  @Override
  public void setInput(RecordIterator incoming) {
    this.incoming = incoming;
    inputRecord = incoming.getRecordPointer();
  }

  protected abstract void consumeRecord();
  protected abstract RecordIterator doWork();

  private RecordIterator consumeData(){
    while (incoming.next() != NextOutcome.NONE_LEFT)  {
      consumeRecord();
    }
    return doWork();
  }
  
  @Override
  protected RecordIterator getIteratorInternal() {
    return new BlockingIterator();
  }

  private class BlockingIterator implements RecordIterator{
    private RecordIterator iter;
    
    public BlockingIterator(){
    }
    
    @Override
    public NextOutcome next() {
      if(this.iter == null){
        this.iter = consumeData();
      }
      return this.iter.next();
    }
    
    @Override
    public ROP getParent() {
      return AbstractBlockingOperator.this;
    }
    
    @Override
    public RecordPointer getRecordPointer() {
      return outputRecord;
    }
    
  }
  
}
