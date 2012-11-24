package org.apache.drill.common.logical.sources.record;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface RecordMaker {
	
	public static final Class<?>[] SUB_TYPES = { FirstRowMaker.class };
}
