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

import org.apache.drill.common.expression.types.DataType;
import org.apache.drill.common.physical.RecordField.ValueMode;
import org.apache.drill.exec.exception.SchemaChangeException;

public class MaterializedField implements Comparable<MaterializedField>{
  private int fieldId;
  private DataType type;
  private boolean nullable;
  private ValueMode mode;
  private Class<?> valueClass;
  
  public MaterializedField(int fieldId, DataType type, boolean nullable, ValueMode mode, Class<?> valueClass) {
    super();
    this.fieldId = fieldId;
    this.type = type;
    this.nullable = nullable;
    this.mode = mode;
    this.valueClass = valueClass;
  }

  public int getFieldId() {
    return fieldId;
  }

  public DataType getType() {
    return type;
  }

  public boolean isNullable() {
    return nullable;
  }

  public ValueMode getMode() {
    return mode;
  }

  public Class<?> getValueClass() {
    return valueClass;
  }

  private void check(String name, Object val1, Object expected) throws SchemaChangeException{
    if(expected.equals(val1)) return;
    throw new SchemaChangeException("Expected and actual field definitions don't match. Actual %s: %s, expected %s: %s", name, val1, name, expected);
  }
  
  public void checkMaterialization(MaterializedField expected) throws SchemaChangeException{
    if(this.type == expected.type || expected.type == DataType.LATEBIND) throw new SchemaChangeException("Expected and actual field definitions don't match. Actual DataType: %s, expected DataTypes: %s", this.type, expected.type);
    if(expected.valueClass != null) check("valueClass", this.valueClass, expected.valueClass);
    check("fieldId", this.fieldId, expected.fieldId);
    check("nullability", this.nullable, expected.nullable);
    check("valueMode", this.mode, expected.mode);
  }

  @Override
  public int compareTo(MaterializedField o) {
    return Integer.compare(this.fieldId, o.fieldId);
  }
  
  
}