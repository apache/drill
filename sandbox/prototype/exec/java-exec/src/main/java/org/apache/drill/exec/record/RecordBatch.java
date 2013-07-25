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
package org.apache.drill.exec.record;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

/**
 * A record batch contains a set of field values for a particular range of records. In the case of a record batch
 * composed of ValueVectors, ideally a batch fits within L2 cache (~256k per core). The set of value vectors do not
 * change unless the next() IterOutcome is a *_NEW_SCHEMA type.
 * 
 * A key thing to know is that the Iterator provided by record batch must align with the rank positions of the field ids
 * provided utilizing getValueVectorId();
 */
public interface RecordBatch extends Iterable<ValueVector> {

  /**
   * Describes the outcome of a RecordBatch being incremented forward.
   */
  public static enum IterOutcome {
    NONE, // No more records were found.
    OK, // A new range of records have been provided.
    OK_NEW_SCHEMA, // A full collection of records
    STOP, // Informs parent nodes that the query has terminated. In this case, a consumer can consume their QueryContext
          // to understand the current state of things.
    NOT_YET // used by batches that haven't received incoming data yet.
  }

  public static enum SetupOutcome {
    OK, OK_NEW_SCHEMA, FAILED
  }

  /**
   * Access the FragmentContext of the current query fragment. Useful for reporting failure information or other query
   * level information.
   * 
   * @return
   */
  public FragmentContext getContext();

  /**
   * Provide the schema of the current RecordBatch. This changes if and only if a *_NEW_SCHEMA IterOutcome is provided.
   * 
   * @return
   */
  public BatchSchema getSchema();

  /**
   * Provide the number of records that are within this record count
   * 
   * @return
   */
  public int getRecordCount();

  /**
   * Inform child nodes that this query should be terminated. Child nodes should utilize the QueryContext to determine
   * what has happened.
   */
  public void kill();

  public abstract SelectionVector2 getSelectionVector2();

  public abstract SelectionVector4 getSelectionVector4();

  /**
   * Get the value vector type and id for the given schema path. The TypedFieldId should store a fieldId which is the
   * same as the ordinal position of the field within the Iterator provided this classes implementation of
   * Iterable<ValueVector>.
   * 
   * @param path
   *          The path where the vector should be located.
   * @return The local field id associated with this vector. If no field matches this path, this will return a null
   *         TypedFieldId
   */
  public abstract TypedFieldId getValueVectorId(SchemaPath path);

  public abstract <T extends ValueVector> T getValueVectorById(int fieldId, Class<?> clazz);

  /**
   * Update the data in each Field reading interface for the next range of records. Once a RecordBatch returns an
   * IterOutcome.NONE, the consumer should no longer next(). Behavior at this point is undetermined and likely to throw
   * an exception.
   * 
   * @return An IterOutcome describing the result of the iteration.
   */
  public IterOutcome next();

  /**
   * Get a writable version of this batch. Takes over owernship of existing buffers.
   * 
   * @return
   */
  public WritableBatch getWritableBatch();

  public static class TypedFieldId {
    final MajorType type;
    final int fieldId;

    public TypedFieldId(MajorType type, int fieldId) {
      super();
      this.type = type;
      this.fieldId = fieldId;
    }

    public MajorType getType() {
      return type;
    }

    public int getFieldId() {
      return fieldId;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      TypedFieldId other = (TypedFieldId) obj;
      if (fieldId != other.fieldId)
        return false;
      if (type == null) {
        if (other.type != null)
          return false;
      } else if (!type.equals(other.type))
        return false;
      return true;
    }

  }

}
