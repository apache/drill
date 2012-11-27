package org.apache.drill.common.logical.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("union")
public class Union extends LogicalOperatorBase {
  private LogicalOperator[] inputs;
  public boolean dedupe;

  @JsonProperty("inputs")
  public void setInputs(LogicalOperator[] inputs) {
    this.inputs = inputs;
    for (LogicalOperator o : inputs) {
      o.registerAsSubscriber(this);
    }
  }

  public LogicalOperator[] getInputs() {
    return inputs;
  }

  @Override
  public int getNestLevel() {
    throw new UnsupportedOperationException();
  }

  
}
