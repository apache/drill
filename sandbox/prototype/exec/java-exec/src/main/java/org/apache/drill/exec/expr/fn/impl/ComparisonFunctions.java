package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.BigIntHolder;
import org.apache.drill.exec.vector.BitHolder;
import org.apache.drill.exec.vector.Float4Holder;
import org.apache.drill.exec.vector.Float8Holder;
import org.apache.drill.exec.vector.IntHolder;
import org.apache.drill.exec.vector.VarBinaryHolder;
import org.apache.drill.exec.vector.VarCharHolder;

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
    
    @FunctionTemplate(name = "equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarBinaryEqual implements DrillSimpleFunc {

        @Param VarBinaryHolder left;
        @Param VarBinaryHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) == 0 ? 1 : 0;
        }
    }
    
    @FunctionTemplate(name = "equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarCharEqual implements DrillSimpleFunc {

        @Param VarCharHolder left;
        @Param VarCharHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) == 0 ? 1 : 0;
        }
    }
    
    @FunctionTemplate(name = "equal", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarBinaryCharEqual implements DrillSimpleFunc {

        @Param VarBinaryHolder left;
        @Param VarCharHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) == 0 ? 1 : 0;
        }
    }
    
    @FunctionTemplate(name = "greater than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarBinaryGTE implements DrillSimpleFunc {

        @Param VarBinaryHolder left;
        @Param VarBinaryHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) > -1 ? 1 : 0;
        }
    }
    
    @FunctionTemplate(name = "greater than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarCharGTE implements DrillSimpleFunc {

        @Param VarCharHolder left;
        @Param VarCharHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) > -1 ? 1 : 0;
        }
    }
    
    @FunctionTemplate(name = "greater than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarBinaryCharGTE implements DrillSimpleFunc {

        @Param VarBinaryHolder left;
        @Param VarCharHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) > -1 ? 1 : 0;
        }
    }
    
    @FunctionTemplate(name = "greater than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarBinaryGT implements DrillSimpleFunc {

        @Param VarBinaryHolder left;
        @Param VarBinaryHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) == 1 ? 1 : 0;
        }
    }
    
    @FunctionTemplate(name = "greater than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarCharGT implements DrillSimpleFunc {

        @Param VarCharHolder left;
        @Param VarCharHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) == 1 ? 1 : 0;
        }
    }
    
    @FunctionTemplate(name = "greater than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarBinaryCharGT implements DrillSimpleFunc {

        @Param VarBinaryHolder left;
        @Param VarCharHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) == 1? 1 : 0;
        }
    }
    
    @FunctionTemplate(name = "less than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarBinaryLTE implements DrillSimpleFunc {

        @Param VarBinaryHolder left;
        @Param VarBinaryHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) < 1 ? 1 : 0;
        }
    }
    
    @FunctionTemplate(name = "less than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarCharLTE implements DrillSimpleFunc {

        @Param VarCharHolder left;
        @Param VarCharHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) < 1 ? 1 : 0;
        }
    }
    
    @FunctionTemplate(name = "less than or equal to", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarBinaryCharLTE implements DrillSimpleFunc {

        @Param VarBinaryHolder left;
        @Param VarCharHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) < 1? 1 : 0;
        }
    }
    
    @FunctionTemplate(name = "less than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarBinaryLT implements DrillSimpleFunc {

        @Param VarBinaryHolder left;
        @Param VarBinaryHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) == -1 ? 1 : 0;
        }
    }
    
    @FunctionTemplate(name = "less than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarCharLT implements DrillSimpleFunc {

        @Param VarCharHolder left;
        @Param VarCharHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) == -1 ? 1 : 0;
        }
    }
    
    @FunctionTemplate(name = "less than", scope = FunctionTemplate.FunctionScope.SIMPLE)
    public static class VarBinaryCharLT implements DrillSimpleFunc {

        @Param VarBinaryHolder left;
        @Param VarCharHolder right;
        @Output BitHolder out;

        public void setup(RecordBatch b) {}

        public void eval() {
            out.value = org.apache.drill.exec.expr.fn.impl.VarHelpers.compare(left, right) == -1 ? 1 : 0;
        }
    }
    
}
