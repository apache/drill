package org.apache.drill.common.logical.data;

import org.apache.drill.common.logical.UnexpectedOperatorType;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * SimpleOperator is an operator that has one inputs at most.
 */
public abstract class SingleInputOperator extends LogicalOperatorBase{

  private LogicalOperator input;
  
  @JsonProperty("input")
  public LogicalOperator getInput() {
    return input;
  }

  @JsonProperty(value="input", required=true)
  public void setInput(LogicalOperator input) {
    if(input instanceof SinkOperator) throw new UnexpectedOperatorType("You have set the input of a sink node of type ["+input.getClass().getSimpleName()+ "] as the input for another node of type ["+this.getClass().getSimpleName()+ "].  This is invalid.");
    this.input = input;
    input.registerAsSubscriber(this);
  }

  @Override
  public int getNestLevel() {
    return input.getNestLevel();
  }
  
  
  
  
}
