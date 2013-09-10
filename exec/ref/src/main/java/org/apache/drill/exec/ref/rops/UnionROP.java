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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Union;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordIterator.NextOutcome;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.exceptions.SetupException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class UnionROP extends ROPBase<Union> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionROP.class);

  private Collection<UnbackedRecord> records;
  private List<RecordIterator> incoming = Lists.newArrayList();
  private Iterator<UnbackedRecord> iterator;
  
  public UnionROP(Union config) {
    super(config);
    // to make things simple, we'll just always make this a blocking operator.
    if(config.isDistinct()){
      records  = Sets.newHashSet();
    }else{
      records = Lists.newArrayList();
    }
  }
  
  @Override
  protected void setupIterators(IteratorRegistry registry) throws SetupException {
    for(LogicalOperator op : config.getInputs()){
      List<RecordIterator> more = registry.getOperator(op);
      if(more.size() != 1) throw new SetupException("Iterator list was incorrect size.");
      incoming.addAll(more);
    }
  }


  protected void doWork() {
    for(RecordIterator ri : incoming){
      RecordPointer rp = ri.getRecordPointer();
      while(ri.next() != NextOutcome.NONE_LEFT){
        UnbackedRecord r = new UnbackedRecord();
        r.copyFrom(rp);
        records.add(r);
      }
    }
    this.iterator = records.iterator();
  }

  @Override
  protected RecordIterator getIteratorInternal() {
    return new ProxyIterator();
  }
  
  private class ProxyIterator implements RecordIterator{
    private ProxySimpleRecord proxyRecord = new ProxySimpleRecord();
    
    @Override
    public RecordPointer getRecordPointer() {
      return proxyRecord;
    }

    @Override
    public NextOutcome next() {
      if(iterator == null) doWork();
      
      if(iterator.hasNext()){
        proxyRecord.setRecord(iterator.next());
        return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
      }else{
        return NextOutcome.NONE_LEFT;
      }
      
    }

    @Override
    public ROP getParent() {
      return UnionROP.this;
    }
    
  }
}
