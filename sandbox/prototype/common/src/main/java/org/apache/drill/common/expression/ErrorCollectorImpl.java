package org.apache.drill.common.expression;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.apache.drill.common.expression.types.DataType;

import java.util.Arrays;
import java.util.List;

public class ErrorCollectorImpl implements ErrorCollector {
    List<ExpressionValidationError> errors;

    public ErrorCollectorImpl() {
        errors = Lists.newArrayList();
    }

    private String addExpr(String expr, String message) {
        return "Expression: [" + expr + "]. Error: " + message;
    }

    @Override
    public void addGeneralError(String expr, String s) {
        errors.add(new ExpressionValidationError(addExpr(expr, s)));
    }

    @Override
    public void addUnexpectedArgumentType(String expr, String name, DataType actual, DataType[] expected, int argumentIndex) {
        errors.add(
                new ExpressionValidationError(
                        addExpr(expr, String.format(
                                "Unexpected argument type. Index :%d Name: %s, Type: %s, Expected type(s): %s",
                                argumentIndex, name, actual, Arrays.toString(expected)
                        ))
                )
        );
    }

    @Override
    public void addUnexpectedArgumentCount(String expr, int actual, Range<Integer> expected) {
        errors.add(new ExpressionValidationError(
                addExpr(expr, String.format("Unexpected argument count. Actual argument count: %d, Expected range: %s", actual, expected))
        ));
    }

    @Override
    public void addUnexpectedArgumentCount(String expr, int actual, int expected) {
        errors.add(new ExpressionValidationError(
                addExpr(expr, String.format("Unexpected argument count. Actual argument count: %d, Expected count: %d", actual, expected))
        ));
    }

    @Override
    public void addNonNumericType(String expr, DataType actual) {
        errors.add(new ExpressionValidationError(
                addExpr(expr, String.format("Unexpected numeric type. Actual type: %s", actual))
        ));
    }

    @Override
    public void addUnexpectedType(String expr, int index, DataType actual) {
        errors.add(new ExpressionValidationError(
                addExpr(expr, String.format("Unexpected argument type. Actual type: %s, Index: %d", actual, index))
        ));
    }

    @Override
    public void addExpectedConstantValue(String expr, int actual, String s) {
        errors.add(new ExpressionValidationError(
                addExpr(expr, String.format("Unexpected constant value. Name: %s, Actual: %s", s, actual))
        ));
    }

    @Override
    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    @Override
    public String toErrorString() {
        return "\n" + Joiner.on("\n").join(errors);
    }
}
