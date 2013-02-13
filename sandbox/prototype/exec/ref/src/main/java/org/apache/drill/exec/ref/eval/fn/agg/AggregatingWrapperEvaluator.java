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

import java.util.List;

import org.apache.drill.exec.ref.eval.EvaluatorTypes.AggregatingEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.values.DataValue;

public class AggregatingWrapperEvaluator implements AggregatingEvaluator {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AggregatingWrapperEvaluator.class);

    private List<AggregatingEvaluator> aggs;
    private BasicEvaluator topEvaluator;

    public AggregatingWrapperEvaluator(List<AggregatingEvaluator> aggs, BasicEvaluator topEvaluator) {
        super();
        this.aggs = aggs;
        this.topEvaluator = topEvaluator;
    }

    @Override
    public DataValue eval() {
        return topEvaluator.eval();
    }

    @Override
    public boolean isConstant() {
        return topEvaluator.isConstant();
    }

    @Override
    public void addRecord() {
        for (AggregatingEvaluator a : aggs) {
            a.addRecord();
        }
    }

    @Override
    public DataValue runningEval() {
        if (topEvaluator instanceof AggregatingEvaluator) {
            return AggregatingEvaluator.class.cast(topEvaluator).runningEval();
        } else {
            return topEvaluator.eval();
        }
    }
}
