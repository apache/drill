package org.apache.drill.common.storage;

import java.nio.ByteBuffer;


public class ValueHolder {
	private int fieldId;
	private int valueLength;
	private int startIndex;
	private ByteBuffer buffer;

	public void setValue(int fieldId, ByteBuffer buffer, int startIndex, int valueLength){
		this.fieldId = fieldId;
		this.valueLength = valueLength;
		this.startIndex = startIndex;
	}
}
