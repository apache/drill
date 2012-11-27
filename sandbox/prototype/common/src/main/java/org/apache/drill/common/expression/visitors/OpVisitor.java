package org.apache.drill.common.expression.visitors;

import org.apache.drill.common.logical.data.LogicalOperator;

public interface OpVisitor {
  public void visit(LogicalOperator o);
}
