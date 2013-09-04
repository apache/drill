package org.apache.drill.common.expression.visitors;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;

public abstract class SimpleExprVisitor<T> implements ExprVisitor<T, Void, RuntimeException>{

  @Override
  public T visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    return visitFunctionCall(call);
  }

  @Override
  public T visitIfExpression(IfExpression ifExpr, Void value) throws RuntimeException {
    return visitIfExpression(ifExpr);
  }

  @Override
  public T visitSchemaPath(SchemaPath path, Void value) throws RuntimeException {
    return visitSchemaPath(path);
  }

  @Override
  public T visitLongConstant(LongExpression intExpr, Void value) throws RuntimeException {
    return visitLongConstant(intExpr);
  }

  @Override
  public T visitDoubleConstant(DoubleExpression dExpr, Void value) throws RuntimeException {
    return visitDoubleConstant(dExpr);
  }

  @Override
  public T visitBooleanConstant(BooleanExpression e, Void value) throws RuntimeException {
    return visitBooleanConstant(e);
  }

  @Override
  public T visitQuotedStringConstant(QuotedString e, Void value) throws RuntimeException {
    return visitQuotedStringConstant(e);
  }

  
  public abstract T visitFunctionCall(FunctionCall call);
  public abstract T visitIfExpression(IfExpression ifExpr);
  public abstract T visitSchemaPath(SchemaPath path);
  public abstract T visitLongConstant(LongExpression intExpr);
  public abstract T visitDoubleConstant(DoubleExpression dExpr);
  public abstract T visitBooleanConstant(BooleanExpression e);
  public abstract T visitQuotedStringConstant(QuotedString e); 
}
