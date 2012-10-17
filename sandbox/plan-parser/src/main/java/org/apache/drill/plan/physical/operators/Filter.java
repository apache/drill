/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.plan.physical.operators;

import org.apache.drill.plan.ast.Arg;
import org.apache.drill.plan.ast.Op;

import java.util.List;
import java.util.Map;

/**
 * Selects records according to whether an expression evaluated on the records is true or non-zero.
 */
public class Filter extends Operator implements DataListener {

    private Operator data;

    public static void define() {
        Operator.defineOperator("filter", Filter.class);
    }

    private EvalOperator filterExpression = null;

    public Filter(Op op, Map<Integer, OperatorReference> bindings) {
        checkArity(op, 2, 1);
        List<Arg> outputs = op.getOutputs();
        if (outputs.size() != 1) {
            throw new IllegalArgumentException("filter operator should only have one output");
        }
        bindings.put(outputs.get(0).asSymbol().getInt(), new OperatorReference(this, 0));
    }

    @Override
    public void link(Op op, Map<Integer, OperatorReference> bindings) {
        List<Arg> inputs = op.getInputs();
        if (inputs.size() != 2) {
            throw new IllegalArgumentException("filter requires two inputs, a filter-expression and a data source.  Got " + inputs.size());
        }
        filterExpression = (EvalOperator) bindings.get(inputs.get(0).asSymbol().getInt()).getOp();
        data = bindings.get(inputs.get(1).asSymbol().getInt()).getOp();
        data.addDataListener(this);
    }

    @Override
    public void notify(Object data) {
        Object r = filterExpression.eval(data);
        if (r instanceof Number) {
            if (((Number) r).doubleValue() != 0) {
                emit(data);
            }
        } else if (r instanceof Boolean) {
            if ((Boolean) r) {
                emit(data);
            }
        } else {
            throw new InvalidData(String.format("Invalid data type %s wanted number or boolean", data.getClass()));
        }
    }

    @Override
    public Schema getSchema() {
        return data.getSchema();
    }
}
