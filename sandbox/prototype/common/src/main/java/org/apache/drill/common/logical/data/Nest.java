package org.apache.drill.common.logical.data;

import java.util.List;

import org.apache.drill.common.expression.LogicalExpression;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("nest")
public class Nest extends LogicalOperatorBase{
  
  
	public LogicalOperator input;
	public String name;
	public List<LogicalExpression> exprs;
	
}
