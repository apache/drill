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

import org.apache.drill.common.logical.data.Limit;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;

public class LimitROP extends SingleInputROPBase<Limit>{

  private LimitIterator iter;
  private Integer first;
  private Integer last;

  public LimitROP(Limit config) {
    super(config);
  }

  @Override
  protected void setupEvals(EvaluatorFactory builder) {
    first = config.getFirst();
    last = config.getLast();
  }

  @Override
  public void setInput(RecordIterator incoming) {
    iter = new LimitIterator(incoming);
  }

  @Override
  public RecordIterator getIteratorInternal() {
    return iter;
  }
  
  private class LimitIterator implements RecordIterator{
    RecordIterator incoming;
    int currentIndex = 0;

    public LimitIterator(RecordIterator incoming) {
      this.incoming = incoming;
    }

    @Override
    public NextOutcome next() {
      NextOutcome r;
      while(true){
        r = incoming.next();
        currentIndex++;
        if (currentIndex > first && (last == null || currentIndex <= last))
            return r;

        if (last != null && currentIndex > last)
            return NextOutcome.NONE_LEFT;
      }
    }

    @Override
    public ROP getParent() {
      return LimitROP.this;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return record;
    }
    
    
    
  }
  
}
