/**
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
package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.record.RecordBatch;

public class ComparisonFunctionsNullable {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ComparisonFunctionsNullable.class);

    private ComparisonFunctionsNullable() {}

    @FunctionTemplate(names = {"equal", "=", "=="}, scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableIntEqual implements DrillSimpleFunc {

        @Param NullableIntHolder left;
        @Param NullableIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            if (left.isSet  == 0 || right.isSet == 0) {
                out.value = 0;
            } else {
                out.value = (left.value == right.value) ? 1 : 0;
            }
        }
    }

    @FunctionTemplate(names = {"equal", "=", "=="}, scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableBigIntEqual implements DrillSimpleFunc {

        @Param NullableBigIntHolder left;
        @Param NullableBigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            if (left.isSet  == 0 || right.isSet == 0) {
                out.value = 0;
            } else {
                out.value = (left.value == right.value) ? 1 : 0;
            }
        }
    }

    @FunctionTemplate(names = {"not equal", "<>", "!="}, scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableIntNotEqual implements DrillSimpleFunc {

        @Param NullableIntHolder left;
        @Param NullableIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            if (left.isSet  == 0 || right.isSet == 0) {
                out.value = 0;
            } else {
                out.value = (left.value != right.value) ? 1 : 0;
            }
        }
    }

    @FunctionTemplate(names = {"not equal", "<>", "!="}, scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableBigIntNotEqual implements DrillSimpleFunc {

        @Param NullableBigIntHolder left;
        @Param NullableBigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            if (left.isSet  == 0 || right.isSet == 0) {
                out.value = 0;
            } else {
                out.value = (left.value != right.value) ? 1 : 0;
            }
        }
    }

    @FunctionTemplate(names = {"less than","<"}, scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableIntLessThan implements DrillSimpleFunc {

        @Param NullableIntHolder left;
        @Param NullableIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            if (left.isSet  == 0 || right.isSet == 0) {
                out.value = 0;
            } else {
                out.value = (left.value < right.value) ? 1 : 0;
            }
        }
    }

    @FunctionTemplate(names = {"less than","<"}, scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableBigIntLessThan implements DrillSimpleFunc {

        @Param NullableBigIntHolder left;
        @Param NullableBigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            if (left.isSet  == 0 || right.isSet == 0) {
                out.value = 0;
            } else {
                out.value = (left.value < right.value) ? 1 : 0;
            }
        }
    }

    @FunctionTemplate(names = {"less than or equal to","<="}, scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableIntLessThanEqual implements DrillSimpleFunc {

        @Param NullableIntHolder left;
        @Param NullableIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            if (left.isSet  == 0 || right.isSet == 0) {
                out.value = 0;
            } else {
                out.value = (left.value <= right.value) ? 1 : 0;
            }
        }
    }

    @FunctionTemplate(names = {"less than or equal to","<="}, scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableBigIntLessThanEqual implements DrillSimpleFunc {

        @Param NullableBigIntHolder left;
        @Param NullableBigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            if (left.isSet  == 0 || right.isSet == 0) {
                out.value = 0;
            } else {
                out.value = (left.value <= right.value) ? 1 : 0;
            }
        }
    }

    @FunctionTemplate(names = {"greater than",">"}, scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableIntGreaterThan implements DrillSimpleFunc {

        @Param NullableIntHolder left;
        @Param NullableIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            if (left.isSet  == 0 || right.isSet == 0) {
                out.value = 0;
            } else {
                out.value = (left.value > right.value) ? 1 : 0;
            }
        }
    }

    @FunctionTemplate(names = {"greater than",">"}, scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableBigIntGreaterThan implements DrillSimpleFunc {

        @Param NullableBigIntHolder left;
        @Param NullableBigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            if (left.isSet  == 0 || right.isSet == 0) {
                out.value = 0;
            } else {
                out.value = (left.value > right.value) ? 1 : 0;
            }
        }
    }

    @FunctionTemplate(names = {"greater than or equal to",">="}, scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableIntGreaterThanEqual implements DrillSimpleFunc {

        @Param NullableIntHolder left;
        @Param NullableIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            if (left.isSet  == 0 || right.isSet == 0) {
                out.value = 0;
            } else {
                out.value = (left.value >= right.value) ? 1 : 0;
            }
        }
    }

    @FunctionTemplate(names = {"greater than or equal to",">="},
                      scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableBigIntGreaterThanEqual implements DrillSimpleFunc {

        @Param NullableBigIntHolder left;
        @Param NullableBigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            if (left.isSet  == 0 || right.isSet == 0) {
                out.value = 0;
            } else {
                out.value = (left.value >= right.value) ? 1 : 0;
            }
        }
    }
}
