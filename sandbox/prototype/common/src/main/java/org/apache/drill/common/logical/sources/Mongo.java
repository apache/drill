package org.apache.drill.common.logical.sources;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("mongo")
public class Mongo extends DataSourceBase{
	public String connection;
}
