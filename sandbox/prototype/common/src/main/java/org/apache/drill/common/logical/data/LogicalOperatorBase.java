package org.apache.drill.common.logical.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.drill.common.logical.ValidationError;

public abstract class LogicalOperatorBase implements LogicalOperator{
	private List<LogicalOperator> children = new ArrayList<LogicalOperator>();
	
	public LogicalOperatorBase(){}
	
	@Override
	public void setupAndValidate(List<LogicalOperator> operators, Collection<ValidationError> errors) {
	}

  @Override
  public void registerAsSubscriber(LogicalOperator operator) {
    children.add(operator);
  }
	
	

}
