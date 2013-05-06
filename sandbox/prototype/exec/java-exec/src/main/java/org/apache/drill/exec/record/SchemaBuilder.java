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

import java.util.Collections;
import java.util.List;

import org.apache.drill.common.expression.types.DataType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.RecordField.ValueMode;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.Lists;

/**
 * A reusable builder that supports the creation of BatchSchemas. Can have a supporting expected object. If the expected Schema object is defined, the
 * builder will always check that this schema is a equal or more materialized version of the current schema.
 */
public class SchemaBuilder {
  private IntObjectOpenHashMap<MaterializedField> fields = new IntObjectOpenHashMap<MaterializedField>();
  private IntObjectOpenHashMap<MaterializedField> expectedFields = new IntObjectOpenHashMap<MaterializedField>();

  private boolean hasSelectionVector;

  public SchemaBuilder(BatchSchema expected) {
    for (MaterializedField f : expected) {
      expectedFields.put(f.getFieldId(), f);
    }
    hasSelectionVector = expected.hasSelectionVector;
  }

  SchemaBuilder() {
  }

  /**
   * Add a field where we don't have type information. In this case, DataType will be set to LATEBIND and valueClass
   * will be set to null.
   * 
   * @param fieldId
   *          The desired fieldId. Should be unique for this BatchSchema.
   * @param nullable
   *          Whether this field supports nullability.
   * @param mode
   * @throws SchemaChangeException
   */
//  public void addLateBindField(short fieldId, boolean nullable, ValueMode mode) throws SchemaChangeException {
//    addTypedField(fieldId, DataType.LATEBIND, nullable, mode, Void.class);
//  }

  public void setSelectionVector(boolean hasSelectionVector) {
    this.hasSelectionVector = hasSelectionVector;
  }

  
//  private void setTypedField(short fieldId, DataType type, boolean nullable, ValueMode mode, Class<?> valueClass)
//      throws SchemaChangeException {
//    MaterializedField f = new MaterializedField(fieldId, type, nullable, mode, valueClass);
//    if (expectedFields != null) {
//      if (!expectedFields.containsKey(f.getFieldId()))
//        throw new SchemaChangeException(
//            String
//                .format(
//                    "You attempted to add a field for Id An attempt was made to add a duplicate fieldId to the schema.  The offending fieldId was %d",
//                    fieldId));
//      f.checkMaterialization(expectedFields.lget());
//    }
//    fields.put(f.getFieldId(), f);
//  }
  
  public void addField(MaterializedField f){
    fields.put(f.getFieldId(), f);
  }

//  public void addTypedField(short fieldId, DataType type, boolean nullable, ValueMode mode, Class<?> valueClass)
//      throws SchemaChangeException {
//    if (fields.containsKey(fieldId))
//      throw new SchemaChangeException(String.format(
//          "An attempt was made to add a duplicate fieldId to the schema.  The offending fieldId was %d", fieldId));
//    setTypedField(fieldId, type, nullable, mode, valueClass);
//  }
//
//  public void replaceTypedField(short fieldId, DataType type, boolean nullable, ValueMode mode, Class<?> valueClass)
//      throws SchemaChangeException {
//    if (!fields.containsKey(fieldId))
//      throw new SchemaChangeException(
//          String.format("An attempt was made to replace a field in the schema, however the schema does " +
//          		"not currently contain that field id.  The offending fieldId was %d", fieldId));
//    setTypedField(fieldId, type, nullable, mode, valueClass);
//  }
  
  public void removeField(short fieldId) throws SchemaChangeException{
    MaterializedField f = fields.remove(fieldId);
    if(f == null) throw new SchemaChangeException("You attempted to remove an nonexistent field.");
  }

  /**
   * Generate a new BatchSchema object based on the current state of the builder.
   * @return
   * @throws SchemaChangeException
   */
  public BatchSchema build() throws SchemaChangeException {
    // check if any fields are unaccounted for.

    List<MaterializedField> fieldList = Lists.newArrayList();
    for (ObjectCursor<MaterializedField> f : fields.values()) {
      if (f != null) fieldList.add(f.value);
    }
    Collections.sort(fieldList);
    return new BatchSchema(this.hasSelectionVector, fieldList);
  }
}