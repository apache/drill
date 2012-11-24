package org.apache.drill.common.logical.data;

import java.util.List;

import org.apache.drill.common.expression.LogicalExpression;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("project")
public class Project extends LogicalOperatorBase{
	public int input;
	public List<LogicalExpression>[] exprs;
}
