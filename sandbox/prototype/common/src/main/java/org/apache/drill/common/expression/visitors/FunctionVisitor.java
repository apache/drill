package org.apache.drill.common.expression.visitors;

import org.apache.drill.common.expression.LogicalExpression;

public abstract class FunctionVisitor {

	public final void visit(LogicalExpression e){
		throw new UnsupportedOperationException("The current visitor doesn't support the provided LogicalExpression of " + e.getClass().getSimpleName());
	}

}
