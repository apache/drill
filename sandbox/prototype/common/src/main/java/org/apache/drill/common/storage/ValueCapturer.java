package org.apache.drill.common.storage;

public class ValueCapturer {
	
	
	/** 
	 * Capture the value of a field.  This is used when the fieldId is already known.
	 * @param fieldId
	 * @param value
	 */
	public void capture(int fieldId, ValueHolder value){
		
	}
	
	/**
	 * Capture the value of a previously unknown field.  This is used when
	 * @param fieldParent
	 * @param fieldName
	 * @param value
	 * @return
	 */
	public int capture(int fieldParent, String fieldName, ValueHolder value){
		return -1;
	}
}
