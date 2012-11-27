package org.apache.drill.common.logical.data;

import java.util.List;

import org.apache.drill.common.expression.LogicalExpression;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("flatten")
public class Flatten extends SingleInputOperator{
  
	public List<LogicalExpression> exprs;
	
}
