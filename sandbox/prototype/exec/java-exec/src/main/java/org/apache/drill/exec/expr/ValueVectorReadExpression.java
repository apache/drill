package org.apache.drill.exec.expr;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.record.RecordBatch.TypedFieldId;

public class ValueVectorReadExpression implements LogicalExpression{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueVectorReadExpression.class);

  private final MajorType type;
  private final int fieldId;
  
  public ValueVectorReadExpression(TypedFieldId tfId) {
    this.type = tfId.getType();
    this.fieldId = tfId.getFieldId();
  }

  @Override
  public MajorType getMajorType() {
    return type;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }

  public int getFieldId() {
    return fieldId;
  }

  @Override
  public ExpressionPosition getPosition() {
    return ExpressionPosition.UNKNOWN;
  }
  
  
}
