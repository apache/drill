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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.drill.common.logical.data.Distinct;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.ref.eval.EvaluatorTypes;
import org.apache.drill.exec.ref.exceptions.SetupException;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.values.ScalarValues;

import java.util.HashSet;
import java.util.Set;

public class DistinctROP extends SingleInputROPBase<Distinct> {
  private static Log logger = LogFactory.getLog(DistinctROP.class);


  private Set<Integer> duplicate = new HashSet<Integer>();
  private FieldReference ref;
  private DataValue currentValue;
  private DataValue previousValue;
  private EvaluatorTypes.BasicEvaluator boundaryKey;
  private EvaluatorTypes.BasicEvaluator evaluator;
  private DistictIter iter;

  public DistinctROP(Distinct config) {
    super(config);
    this.ref = config.getRef();
  }

  @Override
  protected void setupEvals(EvaluatorFactory builder) throws SetupException {
    evaluator = builder.getBasicEvaluator(record, ref);

    if(config.getWithin() != null ){
      boundaryKey = builder.getBasicEvaluator(record, config.getWithin());
    }else{
      boundaryKey = new ScalarValues.IntegerScalar(0);
    }
  }

  @Override
  protected void setInput(RecordIterator incoming) {
    this.iter = new DistictIter(incoming);
  }

  @Override
  protected RecordIterator getIteratorInternal() {
    return iter;
  }

  private class DistictIter implements RecordIterator {
    RecordIterator incoming;

    private DistictIter(RecordIterator incoming) {
      this.incoming = incoming;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return record;
    }

    @Override
    public NextOutcome next() {
      NextOutcome r;
      while (true) {

        previousValue = currentValue;
        r = incoming.next();
        boolean changed = false;

        // read boundary values unless no new values were loaded.
        if(r != NextOutcome.NONE_LEFT){
          currentValue = boundaryKey.eval();
          if(!currentValue.equals(previousValue)) changed = true;

          // skip first boundary.
          if(previousValue == null) changed = false;

        }else{
          changed = true;
        }
        if(changed){
          duplicate.clear();
        }

        if (r == NextOutcome.NONE_LEFT) return NextOutcome.NONE_LEFT;
        DataValue dv = evaluator.eval();
        if (!duplicate.contains(dv.hashCode())) {
          duplicate.add(dv.hashCode());
          return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
        }
      }
    }

    @Override
    public ROP getParent() {
      return DistinctROP.this;
    }
  }
}
