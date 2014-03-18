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
package org.apache.drill.exec.planner.logical;

import java.util.List;

import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelDataTypeFieldImpl;
import org.eigenbase.sql.type.SqlTypeName;

import com.google.common.collect.Lists;

public class RelDataTypeHolder {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RelDataTypeHolder.class);

  List<String> fieldNames = Lists.newArrayList();

  public List<RelDataTypeField> getFieldList(RelDataTypeFactory typeFactory) {

    addStarIfEmpty();

    List<RelDataTypeField> fields = Lists.newArrayList();

    int i = 0;
    for (String fieldName : fieldNames) {

      RelDataTypeField field = new RelDataTypeFieldImpl(fieldName, i, typeFactory.createSqlType(SqlTypeName.ANY));
      fields.add(field);
      i++;
    }

    return fields;
  }

  public int getFieldCount() {
    addStarIfEmpty();
    return fieldNames.size();
  }

  private void addStarIfEmpty(){
    if (fieldNames.isEmpty()) fieldNames.add("*");
  }
  
  public RelDataTypeField getField(RelDataTypeFactory typeFactory, String fieldName) {

    /* First check if this field name exists in our field list */
    int i = 0;
    for (String name : fieldNames) {
      if (name.equalsIgnoreCase(fieldName)) {
        return new RelDataTypeFieldImpl(name, i, typeFactory.createSqlType(SqlTypeName.ANY));
      }
      i++;
    }

    /* This field does not exist in our field list add it */
    RelDataTypeField newField = new RelDataTypeFieldImpl(fieldName, fieldNames.size(),
        typeFactory.createSqlType(SqlTypeName.ANY));

    /* Add the name to our list of field names */
    fieldNames.add(fieldName);

    return newField;
  }

  public List<String> getFieldNames() {
    addStarIfEmpty();
    return fieldNames;
  }
}
