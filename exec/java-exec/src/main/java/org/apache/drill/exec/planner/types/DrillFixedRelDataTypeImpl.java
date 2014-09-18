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

import java.util.Collections;
import java.util.List;

import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelDataTypeFieldImpl;
import org.eigenbase.reltype.RelDataTypeImpl;
import org.eigenbase.reltype.RelDataTypePrecedenceList;
import org.eigenbase.sql.type.SqlTypeExplicitPrecedenceList;
import org.eigenbase.sql.type.SqlTypeName;

import com.google.common.collect.Lists;

/**
 * Implements RowType for fixed field list with ANY type.
 */
public class DrillFixedRelDataTypeImpl extends RelDataTypeImpl {
  private List<RelDataTypeField> fields = Lists.newArrayList();
  private final RelDataTypeFactory typeFactory;

  public DrillFixedRelDataTypeImpl(RelDataTypeFactory typeFactory, List<String> columnNames) {
    this.typeFactory = typeFactory;

    // Add the initial list of columns.
    for (String column : columnNames) {
      addField(column);
    }
    computeDigest();
  }

  private void addField(String columnName) {
    RelDataTypeField newField = new RelDataTypeFieldImpl(
        columnName, fields.size(), typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true));
    fields.add(newField);
  }

  @Override
  public List<RelDataTypeField> getFieldList() {
    return fields;
  }

  @Override
  public int getFieldCount() {
    return fields.size();
  }

  @Override
  public RelDataTypeField getField(String fieldName, boolean caseSensitive) {
    // return the field with given name if available.
    for (RelDataTypeField f : fields) {
      if (fieldName.equalsIgnoreCase(f.getName())) {
        return f;
      }
    }
    return null;
  }

  @Override
  public List<String> getFieldNames() {
    List<String> fieldNames = Lists.newArrayList();
    for (RelDataTypeField f : fields) {
      fieldNames.add(f.getName());
    }

    return fieldNames;
  }

  @Override
  public SqlTypeName getSqlTypeName() {
    return SqlTypeName.ANY;
  }

  @Override
  public RelDataTypePrecedenceList getPrecedenceList() {
    return new SqlTypeExplicitPrecedenceList((List) Collections.emptyList());
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("(DrillFixedRecordRow" + getFieldNames() + ")");
  }

  @Override
  public boolean isStruct() {
    return true;
  }

}
