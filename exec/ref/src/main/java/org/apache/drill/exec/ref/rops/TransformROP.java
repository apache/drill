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

import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Transform;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.ConnectedEvaluator;

public class TransformROP extends SingleInputROPBase<Transform>{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TransformROP.class);
  
  private ConnectedEvaluator[] transformers;
  private RecordIterator iter;

  public TransformROP(Transform transform){
    super(transform);
  }
  
  @Override
  protected void setupEvals(EvaluatorFactory builder) {
    int i =0;
    transformers = new ConnectedEvaluator[config.getTransforms().length];
    for(NamedExpression ne : config.getTransforms()){
      transformers[i] = builder.getConnectedEvaluator(record, ne);
      i++;
    }
  }

  @Override
  public void setInput(RecordIterator incoming) {
    this.iter = new TransformIterator(incoming);
  }
  
  private class TransformIterator implements RecordIterator{
    private final RecordIterator incoming;
    
    public TransformIterator(RecordIterator incoming){
      this.incoming = incoming;
    }
    
    @Override
    public NextOutcome next() {
      NextOutcome outcome = incoming.next();
      if(outcome == NextOutcome.NONE_LEFT){
        return outcome;
      }else{
        for(int i = 0; i < transformers.length; i++){
          transformers[i].eval();
        }
        return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
      }
    }

    @Override
    public ROP getParent() {
      return TransformROP.this;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return record;
    }
    
  }

  @Override
  public RecordIterator getIteratorInternal() {
    return iter;
  }
}
