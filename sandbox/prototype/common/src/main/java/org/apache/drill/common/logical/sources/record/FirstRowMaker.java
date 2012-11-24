package org.apache.drill.common.logical.sources.record;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("first-row")
public class FirstRowMaker extends RecordMakerBase{
	public String delimiter;
}
