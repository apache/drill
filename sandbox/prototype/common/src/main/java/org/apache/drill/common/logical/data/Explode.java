package org.apache.drill.common.logical.data;

import org.apache.drill.common.expression.FieldReference;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * The explode operator 
 */
@JsonTypeName("explode")
public class Explode extends SingleInputOperator{
		
	private final FieldReference ref;

  @JsonCreator
  public Explode(@JsonProperty(value="ref", required=true) FieldReference ref) {
    this.ref = ref;
  }
  
	@JsonCreator
  public Explode(@JsonProperty("input") LogicalOperator input, @JsonProperty("ref") FieldReference ref) {
    this.ref = ref;
    this.setInput(input);
  }
	
  @Override
  public int getNestLevel() {
    return super.getNestLevel()+1;
  }
}
