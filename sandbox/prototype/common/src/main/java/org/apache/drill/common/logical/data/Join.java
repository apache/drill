package org.apache.drill.common.logical.data;

import java.util.List;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.OpVisitor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("join")
public class Join extends LogicalOperatorBase{
	private LogicalOperator left;
	private LogicalOperator right;
	public List<JoinCondition> conditions;
	
	public static class JoinCondition{
		public String relationship;
		@JsonProperty("left") public LogicalExpression leftExpression;
		@JsonProperty("right") public LogicalExpression rightExpression;
	}

  public LogicalOperator getLeft() {
    return left;
  }

  public void setLeft(LogicalOperator left) {
    this.left = left;
    left.registerAsSubscriber(this);
  }

  public LogicalOperator getRight() {
    return right;
  }

  public void setRight(LogicalOperator right) {
    this.right = right;
    right.registerAsSubscriber(this);
  }

  @Override
  public int getNestLevel() {
    return -5;
  }

	
  
	
}
