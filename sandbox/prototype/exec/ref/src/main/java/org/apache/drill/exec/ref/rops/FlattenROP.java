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

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Flatten;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;

public class FlattenROP extends SingleInputROPBase<Flatten> {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectROP.class);

    private final UnbackedRecord outputRecord = new UnbackedRecord();
    private BasicEvaluator evaluator;
    private RecordIterator incoming;

    public FlattenROP(Flatten config) {
        super(config);
    }

    @Override
    protected void setInput(RecordIterator incoming) {
        this.incoming = incoming;
    }

    @Override
    protected RecordIterator getIteratorInternal() {
        return new FlattenIterator();
    }

    @Override
    protected void setupEvals(EvaluatorFactory builder)  {
        evaluator = builder.getBasicEvaluator(record, config.getExpr());
    }


    private class FlattenIterator implements RecordIterator{

        @Override
        public RecordPointer getRecordPointer() {
            return outputRecord;
        }

        @Override
        public NextOutcome next() {
            outputRecord.copyFrom(record);
            outputRecord.addField(config.getName(), evaluator.eval());
            return incoming.next();
            /*if(n != NextOutcome.NONE_LEFT){
                System.out.println("woot");
                return n;
            }
            return NextOutcome.NONE_LEFT;
              */
        }

        @Override
        public ROP getParent() {
            return FlattenROP.this;
        }

    }
}
