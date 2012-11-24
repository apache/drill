package org.apache.drill.common.logical.sources;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property="type")
public interface DataSource {
	
	public static final Class<?>[] SUB_TYPES = {Text.class, Mongo.class, Mysql.class};
	
	public String getName();
	
}
