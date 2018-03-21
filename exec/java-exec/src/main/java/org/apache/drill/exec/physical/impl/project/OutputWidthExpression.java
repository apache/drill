/*
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
 */

package org.apache.drill.exec.physical.impl.project;

import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.exec.expr.AbstractExecExprVisitor;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.fn.output.OutputWidthCalculator;

import java.util.ArrayList;

public abstract class OutputWidthExpression {

    abstract <T, V, E extends Exception> T accept(AbstractExecExprVisitor<T, V, E> visitor, V value) throws E;


    public static class IfElseWidthExpr extends OutputWidthExpression {
        OutputWidthExpression[] expressions;

        public IfElseWidthExpr(OutputWidthExpression ifExpr, OutputWidthExpression elseExpr) {
            this.expressions = new OutputWidthExpression[2];
            this.expressions[0] = ifExpr;
            this.expressions[1] = elseExpr;
        }

        @Override
        public <T, V, E extends Exception> T accept(AbstractExecExprVisitor<T, V, E> visitor, V value) throws E {
            return visitor.visitIfElseWidthExpr(this, value);
        }

    }

    public static class FunctionCallExpr extends OutputWidthExpression {
        FunctionHolderExpression holder;
        ArrayList<OutputWidthExpression> args;
        OutputWidthCalculator widthCalculator;

        public FunctionCallExpr(FunctionHolderExpression holder, OutputWidthCalculator widthCalculator,
                                ArrayList<OutputWidthExpression> args) {
            this.holder = holder;
            this.args = args;
            this.widthCalculator = widthCalculator;
        }

        public FunctionHolderExpression getHolder() {
            return holder;
        }

        public ArrayList<OutputWidthExpression> getArgs() {
            return args;
        }

        public OutputWidthCalculator getCalculator() {
            return widthCalculator;
        }

        @Override
        public <T, V, E extends Exception> T accept(AbstractExecExprVisitor<T, V, E> visitor, V value) throws E {
            return visitor.visitFunctionCallExpr(this, value);
        }
    }

    public static class VarLenReadExpr extends OutputWidthExpression  {
        ValueVectorReadExpression readExpression;
        String name;

        public VarLenReadExpr(ValueVectorReadExpression readExpression) {
            this.readExpression = readExpression;
            this.name = null;
        }

        public VarLenReadExpr(String name) {
            this.readExpression = null;
            this.name = name;
        }

        public ValueVectorReadExpression getReadExpression() {
            return readExpression;
        }

        public String getName() {
            return name;
        }

        @Override
        public <T, V, E extends Exception> T accept(AbstractExecExprVisitor<T, V, E> visitor, V value) throws E {
            return visitor.visitVarLenReadExpr(this, value);
        }
    }

    public static class FixedLenExpr extends OutputWidthExpression {
        int fixedWidth;
        public FixedLenExpr(int fixedWidth) {
            this.fixedWidth = fixedWidth;
        }
        public int getWidth() { return fixedWidth;}

        @Override
        public <T, V, E extends Exception> T accept(AbstractExecExprVisitor<T, V, E> visitor, V value) throws E {
            return visitor.visitFixedLenExpr(this, value);
        }
    }

}
