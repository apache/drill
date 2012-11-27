package org.apache.drill.common.logical.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("scan")
public class Scan extends SourceOperator{
	@JsonProperty("source") public String sourceName;

	public String space;
	
	
  @Override
  public int getNestLevel() {
    return 0;
  }
	
	
}
