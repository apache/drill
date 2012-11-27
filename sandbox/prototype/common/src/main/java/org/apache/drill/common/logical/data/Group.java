package org.apache.drill.common.logical.data;

import java.util.List;

import org.apache.drill.common.expression.LogicalExpression;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("group")
public class Group extends SingleInputOperator{
	public List<LogicalExpression> exprs;

  @Override
  public int getNestLevel() {
    return super.getNestLevel() + exprs.size();
  }
	
	
}
