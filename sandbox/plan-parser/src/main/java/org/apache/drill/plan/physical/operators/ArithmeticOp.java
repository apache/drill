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
 * Operator that works on two numbers.
 */
public abstract class ArithmeticOp extends EvalOperator {
    public static void define() {
        Operator.defineOperator(">", GT.class);
        Operator.defineOperator("==", EQUALS.class);
        Operator.defineOperator("<", LT.class);
        Operator.defineOperator("+", PLUS.class);
        Operator.defineOperator("-", MINUS.class);
        Operator.defineOperator("*", TIMES.class);
        Operator.defineOperator("/", DIVIDE.class);
    }

    public EvalOperator left, right;

    public ArithmeticOp(Op op, Map<Integer, OperatorReference> bindings) {
        checkArity(op, 2, 1);

        // bind our output
        bindings.put(op.getOutputs().get(0).asSymbol().getInt(), new OperatorReference(this, 0));
    }

    @Override
    public Object eval(Object data) {
        Object x = left.eval(data);
        Object y = right.eval(data);

        if (x instanceof Number) {
            if (y instanceof Number) {
                return eval(((Number) x).doubleValue(), ((Number) y).doubleValue());
            } else {
                throw new InvalidData("Expected number but got %s (a %s)", y, y.getClass());
            }
        } else {
            throw new InvalidData("Expected number but got %s (a %s)", x, x.getClass());
        }
    }

    @Override
    public void link(Op op, Map<Integer, OperatorReference> bindings) {
        checkArity(op, 2, 1);

        List<Arg> in = op.getInputs();
        left = extractOperand(in.get(0), bindings);
        right = extractOperand(in.get(1), bindings);
    }

    private EvalOperator extractOperand(Arg arg, Map<Integer, ? extends OperatorReference> bindings) {
        if (arg instanceof Arg.Number) {
            return new ConstantOp(((Arg.Number) arg).doubleValue());
        } else if (arg instanceof Arg.Symbol) {
            return (EvalOperator) bindings.get(arg.asSymbol().getInt()).getOp();
        } else {
            throw new IllegalArgumentException("Wanted constant or reference to another operator");
        }
    }

    public abstract Object eval(double x, double y);

    public static class GT extends ArithmeticOp {
        public GT(Op op, Map<Integer, OperatorReference> bindings) {
            super(op, bindings);
        }

        @Override
        public Object eval(double x, double y) {
            return x > y;
        }
    }

    public static class LT extends ArithmeticOp {
        public LT(Op op, Map<Integer, OperatorReference> bindings) {
            super(op, bindings);
        }

        @Override
        public Object eval(double x, double y) {
            return x < y;
        }
    }

    public static class EQUALS extends ArithmeticOp {
        public EQUALS(Op op, Map<Integer, OperatorReference> bindings) {
            super(op, bindings);
        }

        @Override
        public Object eval(double x, double y) {
            return x == y;
        }
    }

    public static class PLUS extends ArithmeticOp {
        public PLUS(Op op, Map<Integer, OperatorReference> bindings) {
            super(op, bindings);
        }

        @Override
        public Object eval(double x, double y) {
            return x + y;
        }
    }


    public static class MINUS extends ArithmeticOp {
        public MINUS(Op op, Map<Integer, OperatorReference> bindings) {
            super(op, bindings);
        }

        @Override
        public Object eval(double x, double y) {
            return x - y;
        }
    }


    public static class TIMES extends ArithmeticOp {
        public TIMES(Op op, Map<Integer, OperatorReference> bindings) {
            super(op, bindings);
        }

        @Override
        public Object eval(double x, double y) {
            return x * y;
        }
    }


    public static class DIVIDE extends ArithmeticOp {
        public DIVIDE(Op op, Map<Integer, OperatorReference> bindings) {
            super(op, bindings);
        }

        @Override
        public Object eval(double x, double y) {
            return x / y;
        }
    }

}
