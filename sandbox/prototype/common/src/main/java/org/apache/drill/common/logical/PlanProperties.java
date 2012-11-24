package org.apache.drill.common.logical;


public class PlanProperties {
	public String type = "apache_drill_logical_plan";
	public int version;
	public Generator generator = new Generator();
	
	
	public static class Generator{
		public String type;
		public String info;
	}
}
