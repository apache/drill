package org.apache.drill.common.logical.data;

import java.util.List;

import org.apache.drill.common.expression.LogicalExpression;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("order")
public class Order extends SingleInputOperator{

	@JsonProperty("orders") public List<Ordering> orderings;
	
	public static class Ordering {
		public String direction;
		public LogicalExpression expr;
	}
	
	
}
