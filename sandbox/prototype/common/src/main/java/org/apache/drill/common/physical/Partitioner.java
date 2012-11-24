package org.apache.drill.common.physical;

public interface Partitioner {
	public int getPartition();
	public int getPartitionCount();
}
