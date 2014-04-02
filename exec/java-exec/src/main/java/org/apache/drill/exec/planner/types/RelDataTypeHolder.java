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
package org.apache.drill.exec.planner.types;

import java.util.List;

import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelDataTypeFieldImpl;
import org.eigenbase.sql.type.SqlTypeName;

import com.google.common.collect.Lists;

public class RelDataTypeHolder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RelDataTypeHolder.class);

  List<RelDataTypeField> fields = Lists.newArrayList();

  private RelDataTypeFactory typeFactory;
  
  public List<RelDataTypeField> getFieldList(RelDataTypeFactory typeFactory) {
    
    addStarIfEmpty(typeFactory);
    return fields;
  }

  public int getFieldCount() {
    addStarIfEmpty(this.typeFactory);
    return fields.size();
  }

  private void addStarIfEmpty(RelDataTypeFactory typeFactory){
//    RelDataTypeField starCol = getField(typeFactory, "*");
//    if (fields.isEmpty()) fields.add(starCol);
  }
  
  public RelDataTypeField getField(RelDataTypeFactory typeFactory, String fieldName) {

    /* First check if this field name exists in our field list */
    for (RelDataTypeField f : fields) {
      if (fieldName.equalsIgnoreCase(f.getName())) {
        return f;
      }
    }

    /* This field does not exist in our field list add it */
    RelDataTypeField newField = new RelDataTypeFieldImpl(fieldName, fields.size(), typeFactory.createSqlType(SqlTypeName.ANY));

    /* Add the name to our list of field names */
    fields.add(newField);

    return newField;
  }

  public List<String> getFieldNames() {
    List<String> fieldNames = Lists.newArrayList();
    for(RelDataTypeField f : fields){
      fieldNames.add(f.getName());
    };
    
    return fieldNames;
  }
  
  public void setRelDataTypeFactory(RelDataTypeFactory typeFactory) {
    this.typeFactory = typeFactory;
  }
  
}
