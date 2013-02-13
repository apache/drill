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
package org.apache.drill.exec.ref.eval.fn.agg;

import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.eval.fn.FunctionArguments;
import org.apache.drill.exec.ref.eval.fn.FunctionEvaluator;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.NumericValue;
import org.apache.drill.exec.ref.values.ScalarValues;

@FunctionEvaluator("sum")
public class SumAggregator implements AggregatingEvaluator {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SumAggregator.class);
    private boolean constantsOnly;
    private long l = 0;
    private double d = 0;

    boolean integer = true;
    private BasicEvaluator child;

    public SumAggregator(RecordPointer inputRecord, FunctionArguments e) {
        this.child = e.getOnlyEvaluator();
        this.constantsOnly = e.isOnlyConstants();
    }

    @Override
    public void addRecord() {
        DataValue dv = child.eval();
        NumericValue v = dv.getAsNumeric();
        if (integer) {

            switch (v.getNumericType()) {
                case DOUBLE:
                case FLOAT:
                    integer = false;
                    d = l; // loss of precision
                    d += v.getAsDouble();
                    break;
                case INT:
                case LONG:
                    l += v.getAsLong();
                    return;
                default:
                    throw new UnsupportedOperationException();
            }
        } else {
            switch (v.getNumericType()) {
                case DOUBLE:
                case FLOAT:
                case INT:
                case LONG:
                    integer = false;
                    d += v.getAsDouble();
                    return;
                default:
                    throw new UnsupportedOperationException();
            }

        }
    }

    @Override
    public DataValue runningEval() {
        if (integer) {
            return new ScalarValues.LongScalar(l);
        } else {
            return new ScalarValues.DoubleScalar(d);
        }
    }

    @Override
    public DataValue eval() {
        DataValue v = runningEval();
        reset();
        return v;
    }

    private void reset() {
        l = 0;
        d = 0;
        integer = true;
    }

    @Override
    public boolean isConstant() {
        return constantsOnly;
    }

}
