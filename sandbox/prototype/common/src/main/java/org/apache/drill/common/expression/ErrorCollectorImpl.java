package org.apache.drill.common.expression;

import java.util.Arrays;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MajorType;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

public class ErrorCollectorImpl implements ErrorCollector {
    List<ExpressionValidationError> errors;

    public ErrorCollectorImpl() {
        errors = Lists.newArrayList();
    }

    private String addExpr(ExpressionPosition expr, String message) {
        return String.format("Error in expression at index %d.  Error: %s.  Full expression: %s.", expr.getCharIndex(), message, expr.getExpression());
    }

    @Override
    public void addGeneralError(ExpressionPosition expr, String s) {
        errors.add(new ExpressionValidationError(addExpr(expr, s)));
    }

    @Override
    public void addUnexpectedArgumentType(ExpressionPosition expr, String name, MajorType actual, MajorType[] expected, int argumentIndex) {
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
    public void addUnexpectedArgumentCount(ExpressionPosition expr, int actual, Range<Integer> expected) {
        errors.add(new ExpressionValidationError(
                addExpr(expr, String.format("Unexpected argument count. Actual argument count: %d, Expected range: %s", actual, expected))
        ));
    }

    @Override
    public void addUnexpectedArgumentCount(ExpressionPosition expr, int actual, int expected) {
        errors.add(new ExpressionValidationError(
                addExpr(expr, String.format("Unexpected argument count. Actual argument count: %d, Expected count: %d", actual, expected))
        ));
    }

    @Override
    public void addNonNumericType(ExpressionPosition expr, MajorType actual) {
        errors.add(new ExpressionValidationError(
                addExpr(expr, String.format("Unexpected numeric type. Actual type: %s", actual))
        ));
    }

    @Override
    public void addUnexpectedType(ExpressionPosition expr, int index, MajorType actual) {
        errors.add(new ExpressionValidationError(
                addExpr(expr, String.format("Unexpected argument type. Actual type: %s, Index: %d", actual, index))
        ));
    }

    @Override
    public void addExpectedConstantValue(ExpressionPosition expr, int actual, String s) {
        errors.add(new ExpressionValidationError(
                addExpr(expr, String.format("Unexpected constant value. Name: %s, Actual: %s", s, actual))
        ));
    }

    @Override
    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    
    @Override
    public int getErrorCount() {
      return errors.size();
    }

    @Override
    public String toErrorString() {
        return "\n" + Joiner.on("\n").join(errors);
    }

    @Override
    public String toString() {
      return toErrorString();
    }
    
    
}
