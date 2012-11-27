package org.apache.drill.common.logical.data;

import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("transform")
public class Transform extends SingleInputOperator{
  
	public List<Transformation> transforms;
	
	public static class Transformation{
		public FieldReference name;
		public LogicalExpression expr;
	}
}
