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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.Flatten;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.values.BaseArrayValue;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.SimpleArrayValue;


public class FlattenROP extends SingleInputROPBase<Flatten> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FlattenROP.class);

  private RecordPointer outputRecord = new UnbackedRecord();
  private BasicEvaluator evaluator;
  private RecordIterator iter;

  public FlattenROP(Flatten config) {
    super(config);
  }

  @Override
  protected void setInput(RecordIterator incoming) {
    this.iter = new FlattenIterator(incoming);
  }

  @Override
  protected RecordIterator getIteratorInternal() {
    return iter;
  }

  @Override
  protected void setupEvals(EvaluatorFactory builder) {
    evaluator = builder.getBasicEvaluator(record, config.getExpr());
  }

  private class ArrayValueIterator {
    private BaseArrayValue arrayValue;
    private int currentIndex = 0;

    public ArrayValueIterator(BaseArrayValue arrayValue) {
      this.arrayValue = arrayValue;
    }

    public ArrayValueIterator() {
      this(new SimpleArrayValue());
    }

    public DataValue next() {
      DataValue v = null;
      if (currentIndex < arrayValue.size()) {
        v = arrayValue.getByArrayIndex(currentIndex);
      }

      currentIndex++;
      return v;

    }
  }

  private class FlattenIterator implements RecordIterator {
    RecordIterator incoming;
    NextOutcome currentOutcome;
    int currentIndex = 0;
    ArrayValueIterator arrayValueIterator = new ArrayValueIterator();

    public FlattenIterator(RecordIterator incoming) {
      super();
      this.incoming = incoming;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return outputRecord;
    }

    @Override
    public NextOutcome next() {
      DataValue v;
      if ((v = arrayValueIterator.next()) != null)  //if we are already iterating through a sub-array, keep going
        return mergeValue(v);
      else //otherwise, get the next record
        currentOutcome = incoming.next();


      if (currentOutcome != NextOutcome.NONE_LEFT) {
        if (evaluator.eval().getDataType().getMode() == DataMode.REPEATED) {
          arrayValueIterator = new ArrayValueIterator(evaluator.eval().getAsContainer().getAsArray());

          while ((v = arrayValueIterator.next()) != null) {
            return mergeValue(v);
          }
        } else {
          outputRecord.copyFrom(record);
          outputRecord.addField(config.getName(), evaluator.eval());
          if(config.isDrop())
            outputRecord.removeField((SchemaPath)config.getExpr());
        }
      }
      return currentOutcome;
    }

    // helper function to merge one of the values from a sub array into the parent record
    private NextOutcome mergeValue(DataValue v) {
      outputRecord.copyFrom(record);
      outputRecord.addField(config.getName(), v);
      if(config.isDrop())
        outputRecord.removeField((SchemaPath)config.getExpr());
      return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
    }

    @Override
    public ROP getParent() {
      return FlattenROP.this;
    }

  }
}
