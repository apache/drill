package org.apache.drill.common.logical;

import org.apache.drill.common.logical.data.LogicalOperator;

public class UnexpectedOperatorType extends ValidationError{

  public UnexpectedOperatorType(String message){
    super(message);
  }
  
  public <A extends LogicalOperator, B extends LogicalOperator> UnexpectedOperatorType(Class<A> expected, B operator, String message) {
    super(message + " Expected operator of type " + expected.getSimpleName() + " but received operator of type " + operator.getClass().getCanonicalName());
  }


  

  
}
