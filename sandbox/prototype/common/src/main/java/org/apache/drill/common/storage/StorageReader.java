/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
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
