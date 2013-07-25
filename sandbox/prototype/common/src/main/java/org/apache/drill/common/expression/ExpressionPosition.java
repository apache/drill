package org.apache.drill.common.expression;

public class ExpressionPosition {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionPosition.class);
  
  public static final ExpressionPosition UNKNOWN = new ExpressionPosition("--UNKNOWN EXPRESSION--", -1);
  
  private final String expression;
  private final int charIndex;
  
  public ExpressionPosition(String expression, int charIndex) {
    super();
    this.expression = expression;
    this.charIndex = charIndex;
  }

  public String getExpression() {
    return expression;
  }

  public int getCharIndex() {
    return charIndex;
  }
  
  
  
}
