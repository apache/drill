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




import org.eigenbase.reltype.*;
import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlIntervalQualifier;
import org.eigenbase.sql.type.SqlTypeName;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/* We use an instance of this class as the row type for
 * Drill table. Since we don't know the schema before hand
 * whenever optiq requires us to validate that a field exists
 * we always return true and indicate that the type of that
 * field is 'ANY'
 */
public class RelDataTypeDrillImpl extends RelDataTypeImpl {

    private RelDataTypeField defaultField;
    RelDataTypeFactory typeFactory;
    List<RelDataTypeField> drillFieldList = new LinkedList<>();
    List<String> drillfieldNames = new LinkedList<>();


    public RelDataTypeDrillImpl(RelDataTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
        computeDigest();
    }

    @Override
    public List<RelDataTypeField> getFieldList() {

        if (drillFieldList.size() == 0)
        {
            /* By default we only have a single row in drill of 'ANY' type
             * (mainly for select * type queries)
             */
            defaultField = new RelDataTypeFieldImpl("*", 0, typeFactory.createSqlType(SqlTypeName.ANY));

            drillFieldList.add(defaultField);
            drillfieldNames.add("*");
        }
        return drillFieldList;
    }

    
    @Override
    public int getFieldCount() {
        return drillFieldList.size();
    }

//    @Override
//    public int getFieldOrdinal(String fieldName, boolean caseSensitive) {
//
//        /* Get the list of fields and return the
//         * index if the field exists
//         */
//        for (RelDataTypeField field : drillFieldList) {
//            if (field.getName().equals(fieldName))
//                return field.getIndex();
//        }
//
//        /* Couldn't find the field in our list, return -1
//         * Unsure if I should add it to our list of fields
//         */
//        return -1;
//    }

    @Override
    /**
     *
     */
    public RelDataTypeField getField(String fieldName, boolean caseSensitive) {

        /* First check if this field name exists in our field list */
        for (RelDataTypeField field : drillFieldList)
        {
            if (field.getName().equals(fieldName))
                return field;
        }

        /* This field does not exist in our field list add it */
        RelDataTypeField newField = new RelDataTypeFieldImpl(fieldName, drillFieldList.size(), typeFactory.createSqlType(SqlTypeName.ANY));

        /* Add it to the list of fields */
        drillFieldList.add(newField);

        /* Add the name to our list of field names */
        drillfieldNames.add(fieldName);

        return newField;
    }


    @Override
    public List<String> getFieldNames() {

        if (drillfieldNames.size() == 0) {
            drillfieldNames.add("*");
        }

        return drillfieldNames;
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        return null;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
       sb.append("DrillRecordRow");
    }

    @Override
    public boolean isStruct() {
        return true;
    }
}