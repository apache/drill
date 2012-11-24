package org.apache.drill.common.logical;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Transform;
import org.apache.drill.common.logical.sources.DataSource;
import org.apache.drill.common.logical.sources.record.RecordMaker;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

@JsonPropertyOrder({"head", "sources", "query"})
public class LogicalPlan {
	@JsonProperty("head") public PlanProperties properties = new PlanProperties();
	@JsonProperty("sources") public List<DataSource> dataSources = new ArrayList<DataSource>();
	@JsonProperty("query") public List<LogicalOperator> operators = new ArrayList<LogicalOperator>();

	public static void main(String[] args) throws Exception {
		
		
		ObjectMapper mapper = new ObjectMapper();
		
		mapper.enable(SerializationFeature.INDENT_OUTPUT);
		mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		mapper.configure(Feature.ALLOW_COMMENTS, true);

		mapper.registerSubtypes(LogicalOperator.SUB_TYPES);
		mapper.registerSubtypes(DataSource.SUB_TYPES);
		mapper.registerSubtypes(RecordMaker.SUB_TYPES);

    String externalPlan = Files.toString(new File("src/test/resources/simple_plan.json"), Charsets.UTF_8);
    LogicalPlan plan = mapper.readValue(externalPlan, LogicalPlan.class);
    System.out.println(mapper.writeValueAsString(((Transform) plan.operators.get(1)).input));
		System.out.println(mapper.writeValueAsString(plan));	
	}
	
	

}
