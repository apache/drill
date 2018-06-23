/*
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

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Holder for list of RelDataTypeField which may be expanded by implicit columns.
 */
public class ExtendableRelDataTypeHolder extends AbstractRelDataTypeHolder {
  private final List<String> implicitColumnNames;

  public ExtendableRelDataTypeHolder(List<RelDataTypeField> fields, List<String> implicitColumnNames) {
    super(fields);
    this.implicitColumnNames = implicitColumnNames;
  }

  /**
   * Returns RelDataTypeField field with specified name.
   * If field is implicit and absent in the fields list, it will be added.
   *
   * @param typeFactory RelDataTypeFactory which will be used
   *                    for the creation of RelDataType for new fields.
   * @param fieldName   name of the field.
   * @return RelDataTypeField field
   */
  public RelDataTypeField getField(RelDataTypeFactory typeFactory, String fieldName) {

    /* First check if this field name exists in our field list */
    for (RelDataTypeField f : fields) {
      if (fieldName.equalsIgnoreCase(f.getName())) {
        return f;
      }
    }
    RelDataTypeField newField = null;

    if (isImplicitField(fieldName)) {
      // This implicit field does not exist in our field list, add it
      newField = new RelDataTypeFieldImpl(
          fieldName,
          fields.size(),
          typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.VARCHAR), true));
      fields.add(newField);
    }
    return newField;
  }

  /**
   * Checks that specified field is implicit.
   *
   * @param fieldName name of the field which should be checked
   * @return {@code true} if specified filed is implicit
   */
  private boolean isImplicitField(String fieldName) {
    for (String implicitColumn : implicitColumnNames) {
      if (implicitColumn.equalsIgnoreCase(fieldName)) {
        return true;
      }
    }
    return false;
  }
}
