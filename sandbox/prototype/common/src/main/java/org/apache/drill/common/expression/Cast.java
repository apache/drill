package org.apache.drill.common.expression;


@FunctionName("cast")
public class Cast extends LogicalExpressionBase {
  private DataType outputType;
  private LogicalExpression sub;

  public Cast(LogicalExpression subExpression, DataType outputType) throws ExpressionValidationError {
    if (!sub.getDataType().canCastTo(outputType))
      throw new ExpressionValidationError();
    this.sub = subExpression;
    this.outputType = outputType;
  }

  @Override
  public DataType getDataType() {
    return outputType;
  }

  @Override
  public void addToString(StringBuilder sb) {
  }

  
  
}
