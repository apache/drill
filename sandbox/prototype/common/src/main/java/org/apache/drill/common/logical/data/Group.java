package org.apache.drill.common.logical.data;

import java.util.List;

import org.apache.drill.common.expression.LogicalExpression;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("group")
public class Group extends LogicalOperatorBase{
	public int input;
	public List<LogicalExpression> exprs;
}
