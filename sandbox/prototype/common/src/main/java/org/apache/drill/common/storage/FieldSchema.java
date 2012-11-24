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
