package org.apache.drill.common.logical.data;

import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("nest")
public class Nest extends SingleInputOperator{
	public FieldReference ref;
	public List<LogicalExpression> exprs;
	
  @Override
  public int getNestLevel() {
    return super.getNestLevel()-1;
  }
	
	
}
