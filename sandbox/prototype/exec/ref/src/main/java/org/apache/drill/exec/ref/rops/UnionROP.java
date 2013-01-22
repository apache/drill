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
package org.apache.drill.exec.ref.rops;

import java.util.List;

import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Union;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;

public class UnionROP extends ROPBase<LogicalOperator>{
  
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionROP.class);
  
  private List<RecordIterator> incoming;
  private ProxySimpleRecord record;
  
  public UnionROP(Union config) {
    super(config);
  }

  @Override
  protected void setupEvals(EvaluatorFactory builder) {
  }

  @Override
  protected void setupIterators(IteratorRegistry builder) {
    incoming = builder.getOperator(config);
    record.setRecord(incoming.get(0).getRecordPointer());
  }

  @Override
  protected RecordIterator getIteratorInternal() {
    return new MultiIterator();
  }
  
  private class MultiIterator implements RecordIterator{
    private int current = 0;

    @Override
    public NextOutcome next() {
      for(; current < incoming.size(); current++, record.setRecord(incoming.get(current).getRecordPointer()))
      while(current < incoming.size()){
      
        NextOutcome n = incoming.get(current).next();
        if(n != NextOutcome.NONE_LEFT) return n;
        
      }
      return NextOutcome.NONE_LEFT;
    }

    @Override
    public ROP getParent() {
      return UnionROP.this;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return record;
    }
    
  }

}
