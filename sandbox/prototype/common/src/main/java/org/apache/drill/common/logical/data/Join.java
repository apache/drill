package org.apache.drill.common.logical.data;

import java.util.List;

import org.apache.drill.common.expression.LogicalExpression;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("join")
public class Join extends LogicalOperatorBase{
	public int left;
	public int right;
	public List<JoinCondition> conditions;
	
	public static class JoinCondition{
		public String relationship;
		@JsonProperty("left-expr") public LogicalExpression leftExpression;
		@JsonProperty("right-expr") public LogicalExpression rightExpression;
	}
}
