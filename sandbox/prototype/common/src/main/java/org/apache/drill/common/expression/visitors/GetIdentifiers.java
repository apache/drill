package org.apache.drill.common.expression.visitors;

import java.util.HashSet;

import org.apache.drill.common.expression.ValueExpressions;

public class GetIdentifiers extends FunctionVisitor{
	private HashSet<ValueExpressions.Identifier> identifiers = new HashSet<ValueExpressions.Identifier>();
	
	
}
