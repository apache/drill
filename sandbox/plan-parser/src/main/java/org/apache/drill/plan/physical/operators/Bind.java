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
 * How to lookup a variable in an expression.
 */
public class Bind extends EvalOperator {
    private Schema schema = null;

    public static void define() {
        Operator.defineOperator("bind", Bind.class);
    }

    private String name;

    public Bind(Op op, Map<Integer, Operator> bindings) {
        checkArity(op, 2, 1);
        List<Arg> out = op.getOutputs();
        bindings.put(out.get(0).asSymbol().getInt(), this);
    }

    @Override
    public void link(Op op, Map<Integer, Operator> bindings) {
        // connect to our inputs
        name = op.getInputs().get(0).asString();
        schema = bindings.get(op.getInputs().get(1).asSymbol().getInt()).getSchema();
    }

    @Override
    public Object eval(Object data) {
        return schema.get(name, data);
    }
}
