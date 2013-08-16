package org.apache.drill.exec.physical.impl.filter;

import java.util.Iterator;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.MajorType;

import com.google.common.collect.Iterators;

public class ReturnValueExpression implements LogicalExpression{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReturnValueExpression.class);

  private LogicalExpression child;
  private boolean returnTrueOnOne;
  
  public ReturnValueExpression(LogicalExpression child) {
    this(child, true);
  }
  
  public ReturnValueExpression(LogicalExpression child, boolean returnTrueOnOne) {
    this.child = child;
    this.returnTrueOnOne = returnTrueOnOne;
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
  
  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.singletonIterator(child);
  }

  public boolean isReturnTrueOnOne() {
    return returnTrueOnOne;
  }

  
}
