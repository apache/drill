package org.apache.drill.common.logical.data;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("scan")
public class Scan extends LogicalOperatorBase{
	public String source;
	public String name;
	public String table;
}
