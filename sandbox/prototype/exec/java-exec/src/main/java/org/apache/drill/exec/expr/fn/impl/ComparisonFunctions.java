package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.common.expression.ArgumentValidators;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.vector.IntHolder;
import org.apache.drill.exec.vector.BigIntHolder;
import org.apache.drill.exec.vector.Float4Holder;
import org.apache.drill.exec.vector.Float8Holder;
import org.apache.drill.exec.vector.BitHolder;
import org.apache.drill.exec.record.RecordBatch;

public class ComparisonFunctions {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MathFunctions.class);

    private ComparisonFunctions() {}

    @FunctionTemplate(name = "equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class IntEqual implements DrillSimpleFunc {

        @Param IntHolder left;
        @Param IntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value == right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class BigIntEqual implements DrillSimpleFunc {

        @Param BigIntHolder left;
        @Param BigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value == right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class Float4Equal implements DrillSimpleFunc {

        @Param Float4Holder left;
        @Param Float4Holder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value == right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class Float8Equal implements DrillSimpleFunc {

        @Param Float8Holder left;
        @Param Float8Holder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value == right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "not equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class IntNotEqual implements DrillSimpleFunc {

        @Param IntHolder left;
        @Param IntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value != right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "not equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class BigIntNotEqual implements DrillSimpleFunc {

        @Param BigIntHolder left;
        @Param BigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value != right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "not equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class Float4NotEqual implements DrillSimpleFunc {

        @Param Float4Holder left;
        @Param Float4Holder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value != right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "not equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class Float8NotEqual implements DrillSimpleFunc {

        @Param Float8Holder left;
        @Param Float8Holder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value != right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class IntGreaterThan implements DrillSimpleFunc {

        @Param IntHolder left;
        @Param IntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value > right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class BigIntGreaterThan implements DrillSimpleFunc {

        @Param BigIntHolder left;
        @Param BigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value > right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class Float4GreaterThan implements DrillSimpleFunc {

        @Param Float4Holder left;
        @Param Float4Holder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value > right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class Float8GreaterThan implements DrillSimpleFunc {

        @Param Float8Holder left;
        @Param Float8Holder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value > right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class IntGreaterThanEqual implements DrillSimpleFunc {

        @Param IntHolder left;
        @Param IntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value >= right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class BigIntGreaterThanEqual implements DrillSimpleFunc {

        @Param BigIntHolder left;
        @Param BigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value >= right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class Float4GreaterThanEqual implements DrillSimpleFunc {

        @Param Float4Holder left;
        @Param Float4Holder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value >= right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "greater than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class Float8GreaterThanEqual implements DrillSimpleFunc {

        @Param Float8Holder left;
        @Param Float8Holder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value >= right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class IntLessThan implements DrillSimpleFunc {

        @Param IntHolder left;
        @Param IntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value < right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class BigIntLessThan implements DrillSimpleFunc {

        @Param BigIntHolder left;
        @Param BigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value < right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class Float4LessThan implements DrillSimpleFunc {

        @Param Float4Holder left;
        @Param Float4Holder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value < right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class Float8LessThan implements DrillSimpleFunc {

        @Param Float8Holder left;
        @Param Float8Holder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value < right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class IntLessThanEqual implements DrillSimpleFunc {

        @Param IntHolder left;
        @Param IntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value <= right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class BigIntLessThanEqual implements DrillSimpleFunc {

        @Param BigIntHolder left;
        @Param BigIntHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value <= right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class Float4LessThanEqual implements DrillSimpleFunc {

        @Param Float4Holder left;
        @Param Float4Holder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value <= right.value) ? 1 : 0;
        }
    }

    @FunctionTemplate(name = "less than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class Float8LessThanEqual implements DrillSimpleFunc {

        @Param Float8Holder left;
        @Param Float8Holder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = (left.value <= right.value) ? 1 : 0;
        }
    }
}
