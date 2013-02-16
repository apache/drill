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
import org.apache.drill.exec.ref.values.ScalarValues;

@FunctionEvaluator("count")
public class CountAggregator implements AggregatingEvaluator {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CountAggregator.class);

    boolean isConstant;
    long l = 0;
    boolean checkForNull = false;
    BasicEvaluator child;

    public CountAggregator(RecordPointer inputRecord, FunctionArguments e) {
        isConstant = e.isOnlyConstants();
        if (!e.getOnlyEvaluator().isConstant()) {
            checkForNull = true;
            child = e.getOnlyEvaluator();
        }
    }


    @Override
    public void addRecord() {
        if (checkForNull) {
            if (child.eval() != DataValue.NULL_VALUE) {
                l++;
            }
        } else {
            l++;
        }
    }

    @Override
    public DataValue runningEval() {
        return new ScalarValues.LongScalar(l);
    }

    @Override
    public DataValue eval() {
        DataValue v = runningEval();
        l = 0;
        return v;
    }


    @Override
    public boolean isConstant() {
        return isConstant;
    }


}
