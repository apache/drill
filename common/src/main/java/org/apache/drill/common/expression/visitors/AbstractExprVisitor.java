package org.apache.drill.common.expression.visitors;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;

public abstract class AbstractExprVisitor<T, VAL, EXCEP extends Exception> implements ExprVisitor<T, VAL, EXCEP> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractExprVisitor.class);

  @Override
  public T visitFunctionCall(FunctionCall call, VAL value) throws EXCEP {
    return null;
  }

  @Override
  public T visitIfExpression(IfExpression ifExpr, VAL value) throws EXCEP {
    return visitUnknown(ifExpr, value);
  }

  @Override
  public T visitSchemaPath(SchemaPath path, VAL value) throws EXCEP {
    return visitUnknown(path, value);
  }

  @Override
  public T visitLongConstant(LongExpression intExpr, VAL value) throws EXCEP {
    return visitUnknown(intExpr, value);
  }

  @Override
  public T visitDoubleConstant(DoubleExpression dExpr, VAL value) throws EXCEP {
    return visitUnknown(dExpr, value);
  }

  @Override
  public T visitBooleanConstant(BooleanExpression e, VAL value) throws EXCEP {
    return visitUnknown(e, value);
  }

  @Override
  public T visitQuotedStringConstant(QuotedString e, VAL value) throws EXCEP {
    return visitUnknown(e, value);
  }

  @Override
  public T visitUnknown(LogicalExpression e, VAL value) throws EXCEP {
    throw new UnsupportedOperationException(String.format("Expression of type %s not handled by visitor type %s.", e.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
  }
  
  
}
