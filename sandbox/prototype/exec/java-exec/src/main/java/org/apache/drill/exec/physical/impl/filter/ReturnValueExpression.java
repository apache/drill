package org.apache.drill.exec.physical.impl.filter;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.MajorType;

public class ReturnValueExpression implements LogicalExpression{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReturnValueExpression.class);

  private LogicalExpression child;
  
  public ReturnValueExpression(LogicalExpression child) {
    this.child = child;
  }

  public LogicalExpression getChild() {
    return child;
  }

  @Override
  public MajorType getMajorType() {
    return Types.NULL;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }

  @Override
  public ExpressionPosition getPosition() {
    return ExpressionPosition.UNKNOWN;
  }
  
  
  
}
