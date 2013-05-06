package org.apache.drill.exec.record;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Range;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.types.DataType;
import org.apache.drill.common.physical.RecordField;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExpressionTreeMaterializerTest {
    @Test
    public void testMaterializingConstantTree() throws SchemaChangeException {
        ExpressionTreeMaterializer tm = new ExpressionTreeMaterializer();
        ErrorCollector ec = new ErrorCollectorImpl();
        BatchSchema schema = new BatchSchema.BatchSchemaBuilder().buildAndClear();
        LogicalExpression expr = tm.Materialize(new ValueExpressions.LongExpression(1L), schema, ec);
        assertTrue(expr instanceof ValueExpressions.LongExpression);
        assertEquals(1L, ValueExpressions.LongExpression.class.cast(expr).getLong());
        assertFalse(ec.hasErrors());
    }

    @Test
    public void testMaterializingLateboundField() throws SchemaChangeException {
        ExpressionTreeMaterializer tm = new ExpressionTreeMaterializer();
        ErrorCollector ec = new ErrorCollectorImpl();
        BatchSchema.BatchSchemaBuilder builder = new BatchSchema.BatchSchemaBuilder();
        builder.addTypedField((short) 2, DataType.INT64, false, RecordField.ValueMode.RLE, Long.class);
        LogicalExpression expr = tm.Materialize(new FieldReference("test"), builder.buildAndClear(), ec);
        assertEquals(DataType.INT64, expr.getDataType());
        assertFalse(ec.hasErrors());
    }

    @Test
    public void testMaterializingLateboundTree() throws SchemaChangeException {
        ExpressionTreeMaterializer tm = new ExpressionTreeMaterializer();
        ErrorCollector ec = new ErrorCollectorImpl();
        BatchSchema.BatchSchemaBuilder builder = new BatchSchema.BatchSchemaBuilder();
        builder.addTypedField((short) 2, DataType.INT64, false, RecordField.ValueMode.RLE, Long.class);
        LogicalExpression expr = new IfExpression.Builder().addCondition(
                new IfExpression.IfCondition(new FieldReference("test"),
                        new IfExpression.Builder().addCondition(new IfExpression.IfCondition(new ValueExpressions.LongExpression(1L), new FieldReference("test1"))).build()
                )
        ).build();
        LogicalExpression newExpr = tm.Materialize(expr, builder.buildAndClear(), ec);
        assertTrue(newExpr instanceof IfExpression);
        IfExpression newIfExpr = (IfExpression) newExpr;
        assertEquals(1, newIfExpr.conditions.size());
        IfExpression.IfCondition ifCondition = newIfExpr.conditions.get(0);
        assertEquals(DataType.INT64, ifCondition.condition.getDataType());
        assertTrue(ifCondition.expression instanceof IfExpression);
        newIfExpr = (IfExpression) ifCondition.expression;
        assertEquals(1, newIfExpr.conditions.size());
        ifCondition = newIfExpr.conditions.get(0);
        assertEquals(DataType.INT64, ifCondition.expression.getDataType());
        assertEquals(1L, ((ValueExpressions.LongExpression) ifCondition.condition).getLong());
        assertFalse(ec.hasErrors());
    }

    @Test
    public void testMaterializingLateboundTreeValidated() throws SchemaChangeException {
        ExpressionTreeMaterializer tm = new ExpressionTreeMaterializer();
        ErrorCollector ec = new ErrorCollector() {
            boolean errorFound = false;
            @Override
            public void addGeneralError(String expr, String s) {errorFound = true;}
            @Override
            public void addUnexpectedArgumentType(String expr, String name, DataType actual, DataType[] expected, int argumentIndex) {}
            @Override
            public void addUnexpectedArgumentCount(String expr, int actual, Range<Integer> expected) {}
            @Override
            public void addUnexpectedArgumentCount(String expr, int actual, int expected) {}
            @Override
            public void addNonNumericType(String expr, DataType actual) {}
            @Override
            public void addUnexpectedType(String expr, int index, DataType actual) {}
            @Override
            public void addExpectedConstantValue(String expr, int actual, String s) {}
            @Override
            public boolean hasErrors() { return errorFound; }
            @Override
            public String toErrorString() { return ""; }
        };
        BatchSchema.BatchSchemaBuilder builder = new BatchSchema.BatchSchemaBuilder();
        builder.addTypedField((short) 2, DataType.INT64, false, RecordField.ValueMode.RLE, Long.class);
        LogicalExpression expr = new FunctionCall(FunctionDefinition.simple("testFunc", new ArgumentValidator() {
            @Override
            public void validateArguments(String expr, List<LogicalExpression> expressions, ErrorCollector errors) {
                errors.addGeneralError(expr, "Error!");
            }

            @Override
            public String[] getArgumentNamesByPosition() {
                return new String[0];
            }
        }, OutputTypeDeterminer.FIXED_BOOLEAN), Lists.newArrayList((LogicalExpression) new FieldReference("test")));
        LogicalExpression newExpr = tm.Materialize(expr, builder.buildAndClear(), ec);
        assertTrue(newExpr instanceof FunctionCall);
        FunctionCall funcExpr = (FunctionCall) newExpr;
        assertEquals(1, funcExpr.args.size());
        assertEquals(DataType.INT64, funcExpr.args.get(0).getDataType());
        assertTrue(ec.hasErrors());
    }
}
