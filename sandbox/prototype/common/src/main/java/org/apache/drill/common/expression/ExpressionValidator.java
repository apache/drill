package org.apache.drill.common.expression;

import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;

public class ExpressionValidator implements ExprVisitor<Void, ErrorCollector, RuntimeException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionValidator.class);

  @Override
  public Void visitFunctionCall(FunctionCall call, ErrorCollector errors) throws RuntimeException {
    call.getDefinition().getArgumentValidator()
        .validateArguments(call.getPosition(), call.args, errors);
    return null;
  }

  @Override
  public Void visitIfExpression(IfExpression ifExpr, ErrorCollector errors) throws RuntimeException {
    // confirm that all conditions are required boolean values.
    int i = 0;
    for (IfCondition c : ifExpr.conditions) {
      MajorType mt = c.condition.getMajorType();
      if (mt.getMode() != DataMode.REQUIRED || mt.getMinorType() != MinorType.BOOLEAN){
        errors.addGeneralError(c.condition.getPosition(),String.format(
                        "Failure composing If Expression.  All conditions must return a required value and be of type boolean.  Condition %d was DatMode %s and Type %s.",
                        i, mt.getMode(), mt.getMinorType()));
      }
      i++;
    }

    // confirm that all outcomes are the same type.
    final MajorType mt = ifExpr.elseExpression.getMajorType();
    i = 0;
    for (IfCondition c : ifExpr.conditions) {
      MajorType innerT = c.expression.getMajorType();
      if (
          (innerT.getMode() == DataMode.REPEATED && mt.getMode() != DataMode.REPEATED) || //
          (innerT.getMinorType() != mt.getMinorType())
          ) {
        errors.addGeneralError(c.condition.getPosition(),String.format(
            "Failure composing If Expression.  All expressions must return the same MajorType as the else expression.  The %d if condition returned type type %s but the else expression was of type %s",
            i, innerT, mt));
      }
      i++;
    }
    return null;
  }

  @Override
  public Void visitSchemaPath(SchemaPath path, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitLongConstant(LongExpression intExpr, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitDoubleConstant(DoubleExpression dExpr, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitBooleanConstant(BooleanExpression e, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitQuotedStringConstant(QuotedString e, ErrorCollector errors) throws RuntimeException {
    return null;
  }

  @Override
  public Void visitUnknown(LogicalExpression e, ErrorCollector value) throws RuntimeException {
    return null;
  }

  
}
