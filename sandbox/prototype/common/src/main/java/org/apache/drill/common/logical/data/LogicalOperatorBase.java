package org.apache.drill.common.logical.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.visitors.OpVisitor;
import org.apache.drill.common.logical.ValidationError;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

public abstract class LogicalOperatorBase implements LogicalOperator{
	private List<LogicalOperator> children = new ArrayList<LogicalOperator>();
	@JsonInclude(Include.NON_DEFAULT) public String memo;
	
	public LogicalOperatorBase(){}
	
	@Override
	public void setupAndValidate(List<LogicalOperator> operators, Collection<ValidationError> errors) {
	}

  @Override
  public void registerAsSubscriber(LogicalOperator operator) {
    children.add(operator);
  }

  @Override
  public void accept(OpVisitor visitor) {
    visitor.visit(this);
    for(LogicalOperator o : children){
      visitor.visit(o);
    }
  }

  @Override
  public Iterator<LogicalOperator> iterator() {
    return children.iterator();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " [memo=" + memo + "]";
  }

  
  
 
	

}
