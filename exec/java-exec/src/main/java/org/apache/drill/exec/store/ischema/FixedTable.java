/**
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
 */
package org.apache.drill.exec.store.ischema;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.memory.BufferAllocator;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;
import org.eigenbase.sql.type.SqlTypeName;

/**
 * A FixedTable represents a table where the fields are always the same name and type.
 * Since the names and types are unchanging, it is easy to create value vectors during startup
 * and to use strong types when storing values in the vectors.
 */
public class FixedTable extends EmptyVectorSet  {
  String tableName;
  String[] fieldNames;
  MajorType[] fieldTypes;

  /* (non-Javadoc)
   * @see org.apache.drill.exec.store.ischema.VectorSet#createVectors(org.apache.drill.exec.memory.BufferAllocator)
   */
  @Override
  public void createVectors(BufferAllocator allocator) {
    createVectors(fieldNames, fieldTypes, allocator);
  }
  
  /**
   * Construct a generic table with an unchanging schema. 
   * We leave it to subclasses to define the fields and types.
   * @param tableName - name of the table
   * @param fieldNames - names of the fields
   * @param fieldTypes - major types of the fields
   */
  FixedTable(String tableName, String[] fieldNames, MajorType[] fieldTypes) {
    this.tableName = tableName;
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
  }
  
  public String getName() {
    return tableName;
  }
  
  
  
  /**
   * Helper function to get the Optiq Schema type from a Drill Type.
   * Again, we only do it for the information schema types, so it needs to be generalized.
   * (This probably already exists elsewhere.)
   */
  
  static public RelDataType getRelDataType(RelDataTypeFactory typeFactory, MajorType type) {
    switch (type.getMinorType()) {
    case INT:    return typeFactory.createSqlType(SqlTypeName.INTEGER);
    case VARCHAR: return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    default: return null; // TODO - throw exception?
    }
  }
    
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    
    // Convert the array of Drill types to an array of Optiq types
    RelDataType[] relTypes = new RelDataType[fieldTypes.length];
    for (int i=0; i<fieldTypes.length; i++) {
      relTypes[i] = getRelDataType(typeFactory, fieldTypes[i]);
    }
    
    // Create a struct type to represent the 
    return typeFactory.createStructType(relTypes, fieldNames);
  }

  
  

}
