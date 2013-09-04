package org.apache.drill.common.expression;

import java.util.List;

public class NoArgValidator implements ArgumentValidator{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NoArgValidator.class);

  @Override
  public void validateArguments(ExpressionPosition expr, List<LogicalExpression> expressions, ErrorCollector errors) {
    if(!expressions.isEmpty()){
      errors.addGeneralError(expr, "Expected zero arguments, received more than that.");
    }
  }

  @Override
  public String[] getArgumentNamesByPosition() {
    return new String[0];
  }
  
  public static final NoArgValidator VALIDATOR = new NoArgValidator();
}
