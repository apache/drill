package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.common.expression.ArgumentValidators;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.vector.IntHolder;
import org.apache.drill.exec.vector.BigIntHolder;
import org.apache.drill.exec.vector.BitHolder;
import org.apache.drill.exec.record.RecordBatch;

public class ComparisonFunctions {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MathFunctions.class);

    private ComparisonFunctions() {}

    @FunctionTemplate(name = "equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class IntEqual implements DrillFunc {

        @Param IntHolder left;
        @Param IntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value == right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class LongEqual implements DrillFunc {

        @Param BigIntHolder left;
        @Param BigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value == right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "not equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class IntNotEqual implements DrillFunc {

        @Param IntHolder left;
        @Param IntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value != right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "not equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class LongNotEqual implements DrillFunc {

        @Param BigIntHolder left;
        @Param BigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value != right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class IntGreaterThan implements DrillFunc {

        @Param IntHolder left;
        @Param IntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value > right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class LongGreaterThan implements DrillFunc {

        @Param BigIntHolder left;
        @Param BigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value > right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class IntGreaterThanEqual implements DrillFunc {

        @Param IntHolder left;
        @Param IntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value >= right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class LongGreaterThanEqual implements DrillFunc {

        @Param BigIntHolder left;
        @Param BigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value >= right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class IntLessThan implements DrillFunc {

        @Param IntHolder left;
        @Param IntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value < right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class LongLessThan implements DrillFunc {

        @Param BigIntHolder left;
        @Param BigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value < right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class IntLessThanEqual implements DrillFunc {

        @Param IntHolder left;
        @Param IntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value <= right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class LongLessThanEqual implements DrillFunc {

        @Param BigIntHolder left;
        @Param BigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value <= right.value) ? 1 : 0;
        }
    }
}
