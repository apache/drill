package org.apache.drill.common.logical.data;

import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("project")
public class Project extends SinkOperator{
  
  @JsonProperty("exprs") public List<FieldSelection> selections;
  
  public static class FieldSelection {
    public LogicalExpression expr;
    public FieldReference ref;
  }
	
}
