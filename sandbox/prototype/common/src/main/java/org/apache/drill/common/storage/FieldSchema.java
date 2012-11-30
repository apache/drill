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

/**
 * Holds the schema of the incoming data.  Each row batch carries a particular schema.  In the case of a mid row batch schema change, the provided schema is a union of the found schemas.  As the schema changes between row batches, the schema id increments.  New schemas should be provided the first time their schema is changed.  Each scanner is provided with a unique schema shift.  When multiple scanner data is merged, so are the schemas.
 * @author jnadeau
 *
 */
public class FieldSchema {

	/**
	 * Stores the set of provided schemas.  Is in a protobuf binary format.  Each object is:
	 *   parentField: int32
	 *   format: value type
	 *   name: string 
	 *   
	 */
	private ByteBuffer schemaHolder;
	private int currentSchemaPosition;
	private long currentSchemaId;
	
	public void mergeSchema(FieldSchema schema){
		
	}
	
	public static FieldSchema generateMerged(FieldSchema...schemas){
		return null;
	}
}
