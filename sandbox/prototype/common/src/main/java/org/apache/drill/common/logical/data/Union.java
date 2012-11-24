package org.apache.drill.common.logical.data;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("union")
public class Union extends LogicalOperatorBase{
	public int[] inputs;
	public boolean dedupe;
}
