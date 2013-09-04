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

import org.apache.drill.common.logical.data.Filter;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BooleanEvaluator;

public class FilterROP extends SingleInputROPBase<Filter>{

  private FilterIterator iter;
  private BooleanEvaluator filterEval;
  
  public FilterROP(Filter config) {
    super(config);
  }

  @Override
  protected void setupEvals(EvaluatorFactory builder) {
    filterEval = builder.getBooleanEvaluator(record, config.getExpr());
  }

  @Override
  public void setInput(RecordIterator incoming) {
    iter = new FilterIterator(incoming);
  }

  @Override
  public RecordIterator getIteratorInternal() {
    return iter;
  }
  
  private class FilterIterator implements RecordIterator{
    RecordIterator incoming;

    public FilterIterator(RecordIterator incoming) {
      this.incoming = incoming;
    }

    @Override
    public NextOutcome next() {
      NextOutcome r;
      while(true){
        r = incoming.next();
        if(r == NextOutcome.NONE_LEFT) return NextOutcome.NONE_LEFT;
        if(filterEval.eval()) return r;
      }
    }

    @Override
    public ROP getParent() {
      return FilterROP.this;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return record;
    }
    
    
    
  }
  
}
