package org.apache.drill.common.storage;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public interface StorageReader {

		public static enum Outcome {
			NO_MORE_DATA, AT_TREE_BOUNDARY, WITHIN_TREE, FATAL_ERROR
		}
		
		
		
		
		/**
		 * 
		 * @param dataBuffer
		 * @param positionBuffer
		 * @return
		 */
		public abstract Outcome getMoreData(ByteBuffer dataBuffer, IntBuffer positionBuffer);
		
		
		public abstract void registerByFieldOrdinal(int ordinal, Object fieldHandler);		
}
