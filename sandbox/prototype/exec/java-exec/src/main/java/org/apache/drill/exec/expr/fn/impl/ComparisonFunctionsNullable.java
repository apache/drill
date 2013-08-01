package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.NullableBigIntHolder;
import org.apache.drill.exec.vector.BitHolder;
import org.apache.drill.exec.vector.NullableIntHolder;

public class ComparisonFunctionsNullable {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ComparisonFunctionsNullable.class);

    private ComparisonFunctionsNullable() {}

    @FunctionTemplate(name = "equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableIntEqual implements DrillFunc {

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

    @FunctionTemplate(name = "equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableBigIntEqual implements DrillFunc {

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

    @FunctionTemplate(name = "not equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableIntNotEqual implements DrillFunc {

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

    @FunctionTemplate(name = "not equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class NullableBigIntNotEqual implements DrillFunc {

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
}
