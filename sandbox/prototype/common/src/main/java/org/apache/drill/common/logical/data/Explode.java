package org.apache.drill.common.logical.data;

import org.apache.drill.common.expression.FieldReference;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * The explode operator 
 */
@JsonTypeName("explode")
public class Explode extends SingleInputOperator{
		
	@JsonProperty("ref") public FieldReference reference;
	
	public Explode(){}
	
}
