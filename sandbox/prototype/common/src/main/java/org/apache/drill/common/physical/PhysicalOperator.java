package org.apache.drill.common.physical;


public class PhysicalOperator {
	
	public static enum IteratorOutcome{
		OK, NONE, CANCEL;
	}
	
	private StreamSource[] sources;
	private StreamSink[] sinks;
	private Partitioner partitioner;
	
	public void setup(){
		
	}
	
	public IteratorOutcome getNext(){
		return IteratorOutcome.CANCEL;
	}
	
	public void teardown(){
		
	}
}
