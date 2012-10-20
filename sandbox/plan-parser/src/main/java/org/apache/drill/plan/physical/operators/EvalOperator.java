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

import org.apache.drill.plan.ast.Op;

import java.util.Map;

/**
 * Describes a scalar expression.
 */
public abstract class EvalOperator extends Operator {
    public EvalOperator(Op op, Map<Integer, Operator> bindings, int inputArgs, int outputArgs) {
        super(op, bindings, inputArgs, outputArgs);
    }

    // only for Constants
    protected EvalOperator() {
        super();
    }

    public abstract Object eval(Object data);

    @Override
    public Schema getSchema() {
        throw new UnsupportedOperationException("Can't get schema from expression");
    }
}
