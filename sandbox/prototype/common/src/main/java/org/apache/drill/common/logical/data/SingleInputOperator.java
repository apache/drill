package org.apache.drill.common.logical.data;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * SimpleOperator is an operator that has one inputs at most.
 */
public abstract class SingleInputOperator extends LogicalOperatorBase{

  public LogicalOperator input;
  
  @JsonProperty
  public void setInput(LogicalOperator input){
    this.input = input;
    input.registerAsSubscriber(this);
  }
}
