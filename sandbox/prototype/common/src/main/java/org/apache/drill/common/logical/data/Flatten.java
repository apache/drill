package org.apache.drill.common.logical.data;

import java.util.List;

import org.apache.drill.common.expression.LogicalExpression;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Flatten {
	@JsonProperty("input") public int inputNode;
	public List<LogicalExpression> exprs;
}
