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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.RunningAggregate;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.eval.EvaluatorTypes;
import org.apache.drill.exec.ref.exceptions.SetupException;
import org.apache.drill.exec.ref.values.DataValue;

import java.util.List;

public class RunningAggregateROP extends SingleInputROPBase<RunningAggregate> {
    private RecordIterator incoming;
    private ProxySimpleRecord record;
    private DataValue lastFoundBoundry;
    private EvaluatorTypes.BasicEvaluator boundryEval;
    private EvaluatorTypes.AggregatingEvaluator[] evals;
    private FieldReference[] outputRefs;

    public RunningAggregateROP(RunningAggregate config) {
        super(config);
        record = new ProxySimpleRecord();
    }

    @Override
    protected void setupEvals(final EvaluatorFactory builder) throws SetupException {
        super.setupEvals(builder);
        if (config.getWithin() != null) {
            boundryEval = builder.getBasicEvaluator(incoming.getRecordPointer(), config.getWithin());
        }

        NamedExpression[] aggregations = config.getAggregations();
        int evalsLength = aggregations.length;
        evals = new EvaluatorTypes.AggregatingEvaluator[evalsLength];
        outputRefs = new FieldReference[aggregations.length];

        for(int i = 0; i < evalsLength; ++i) {
            evals[i] = builder.getAggregatingOperator(incoming.getRecordPointer(), aggregations[i].getExpr());
            outputRefs[i] = aggregations[i].getRef();
        }
    }

    @Override
    protected void setInput(RecordIterator incoming) {
        this.incoming = incoming;
        record.setRecord(incoming.getRecordPointer());
    }

    @Override
    protected RecordIterator getIteratorInternal() {
        return new Iterator();
    }

    private class Iterator implements RecordIterator {
        @Override
        public RecordPointer getRecordPointer() {
            return record;
        }

        @Override
        public NextOutcome next() {
            NextOutcome outcome = incoming.next();
            if (outcome == NextOutcome.NONE_LEFT) {
                return outcome;
            }

            if (boundryEval != null) {
                DataValue boundryValue = boundryEval.eval();
                if (!boundryValue.equals(lastFoundBoundry)) {
                    for (EvaluatorTypes.AggregatingEvaluator eval : evals) {
                        eval.eval();
                    }
                    lastFoundBoundry = boundryValue;
                }
            }

            for (int i = 0; i < evals.length; ++i) {
                EvaluatorTypes.AggregatingEvaluator aggregatingEvaluator = evals[i];
                aggregatingEvaluator.addRecord();
                record.addField(outputRefs[i], aggregatingEvaluator.runningEval());
            }

            return outcome;
        }

        @Override
        public ROP getParent() {
            return RunningAggregateROP.this;
        }
    }
}
