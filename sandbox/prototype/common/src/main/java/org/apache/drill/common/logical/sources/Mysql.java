package org.apache.drill.common.logical.sources;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("mysql")
public class Mysql extends DataSourceBase{
	public String connection;
}
