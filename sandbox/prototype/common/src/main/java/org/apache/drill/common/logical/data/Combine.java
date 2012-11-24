package org.apache.drill.common.logical.data;

import com.fasterxml.jackson.annotation.JsonTypeName;


@JsonTypeName("combine")
public class Combine extends SingleInputOperator{

	public String name;
	
	
}
