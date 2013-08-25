package org.apache.drill.exec.expr;

import java.util.Iterator;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.TypedFieldId;

import com.google.common.collect.Iterators;

public class ValueVectorWriteExpression implements LogicalExpression {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueVectorWriteExpression.class);

  private final TypedFieldId fieldId;
  private final LogicalExpression child;
  private final boolean safe;
  
  public ValueVectorWriteExpression(TypedFieldId fieldId, LogicalExpression child){
    this(fieldId, child, false);
  }
  
  public ValueVectorWriteExpression(TypedFieldId fieldId, LogicalExpression child, boolean safe){
    this.fieldId = fieldId;
    this.child = child;
    this.safe = safe;
  }
  
  public TypedFieldId getFieldId() {
    return fieldId;
  }

  @Override
  public MajorType getMajorType() {
    return Types.NULL;
  }

  
  public boolean isSafe() {
    return safe;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }

  @Override
  public ExpressionPosition getPosition() {
    return ExpressionPosition.UNKNOWN;
  }

  public LogicalExpression getChild() {
    return child;
  }
  
  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.singletonIterator(child);
  }

  
}
