package org.apache.drill.exec.physical.impl.sort;

import java.util.List;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.exec.expr.ValueVectorReadExpression;

import com.google.common.collect.Lists;

public class ReadIndexRewriter implements ExprVisitor<LogicalExpression, String, RuntimeException> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReadIndexRewriter.class);

  private String batchName;
  
  
  @Override
  public LogicalExpression visitUnknown(LogicalExpression e, String newIndexName) {
    if (e instanceof ValueVectorReadExpression) {
      ValueVectorReadExpression old = (ValueVectorReadExpression) e;
      return new ValueVectorReadExpression(old.getTypedFieldId(), newIndexName);
    } else {
      throw new UnsupportedOperationException(String.format(
          "ReadIndex rewriter doesn't know how to rewrite expression of type %s.", e.getClass().getName()));
    }
  }

  @Override
  public LogicalExpression visitFunctionCall(FunctionCall call, String newIndexName) {
    List<LogicalExpression> args = Lists.newArrayList();
    for (int i = 0; i < call.args.size(); ++i) {
      LogicalExpression newExpr = call.args.get(i).accept(this, null);
      args.add(newExpr);
    }

    return new FunctionCall(call.getDefinition(), args, call.getPosition());
  }

  @Override
  public LogicalExpression visitIfExpression(IfExpression ifExpr, String newIndexName) {
    List<IfExpression.IfCondition> conditions = Lists.newArrayList(ifExpr.iterator());
    LogicalExpression newElseExpr = ifExpr.elseExpression.accept(this, null);

    for (int i = 0; i < conditions.size(); ++i) {
      IfExpression.IfCondition condition = conditions.get(i);

      LogicalExpression newCondition = condition.condition.accept(this, null);
      LogicalExpression newExpr = condition.expression.accept(this, null);
      conditions.set(i, new IfExpression.IfCondition(newCondition, newExpr));
    }

    return IfExpression.newBuilder().setElse(newElseExpr).addConditions(conditions).build();
  }

  @Override
  public LogicalExpression visitSchemaPath(SchemaPath path, String newIndexName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public LogicalExpression visitLongConstant(LongExpression intExpr, String value) throws RuntimeException {
    return intExpr;
  }

  @Override
  public LogicalExpression visitDoubleConstant(DoubleExpression dExpr, String value) throws RuntimeException {
    return dExpr;
  }

  @Override
  public LogicalExpression visitBooleanConstant(BooleanExpression e, String value) throws RuntimeException {
    return e;
  }

  @Override
  public LogicalExpression visitQuotedStringConstant(QuotedString e, String value) throws RuntimeException {
    return e;
  }

}
