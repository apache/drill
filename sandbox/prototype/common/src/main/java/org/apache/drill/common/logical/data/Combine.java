package org.apache.drill.common.logical.data;

import org.apache.drill.common.expression.FieldReference;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;


@JsonTypeName("combine")
public class Combine extends SingleInputOperator{
  private final FieldReference ref;

  @JsonCreator
  public Combine(@JsonProperty("ref") FieldReference ref) {
    this.ref = ref;
  }
  
  @JsonCreator
  public Combine(@JsonProperty("input") LogicalOperator input, @JsonProperty("ref") FieldReference ref) {
    this.ref = ref;
    this.setInput(input);
  }

  @JsonProperty("ref")
  public FieldReference getRef() {
    return ref;
  }

  @Override
  public int getNestLevel() {
    return super.getNestLevel()-1;
  }
  
  
  
   
}
