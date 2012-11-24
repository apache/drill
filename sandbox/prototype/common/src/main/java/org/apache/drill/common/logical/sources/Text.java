package org.apache.drill.common.logical.sources;

import java.util.List;

import org.apache.drill.common.logical.sources.record.RecordMaker;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("text")
public class Text extends DataSourceBase{
	public List<String> files;
	@JsonProperty("compress") public String compressionType;
	@JsonProperty("line-delimiter") public String lineDelimiter;
	@JsonProperty("record-maker") public RecordMaker recordMaker;
}
