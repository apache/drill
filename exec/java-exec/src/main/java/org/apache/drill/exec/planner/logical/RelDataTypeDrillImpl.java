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
import org.eigenbase.reltype.RelDataTypeImpl;
import org.eigenbase.sql.type.SqlTypeName;

/* We use an instance of this class as the row type for
 * Drill table. Since we don't know the schema before hand
 * whenever optiq requires us to validate that a field exists
 * we always return true and indicate that the type of that
 * field is 'ANY'
 */
public class RelDataTypeDrillImpl extends RelDataTypeImpl {

    private final RelDataTypeFactory typeFactory;
    private final RelDataTypeHolder holder;
    
    public RelDataTypeDrillImpl(RelDataTypeHolder holder, RelDataTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
        this.holder = holder;
        computeDigest();
    }
    
    @Override
    public List<RelDataTypeField> getFieldList() {
      return holder.getFieldList(typeFactory);
    }
    
    @Override
    public int getFieldCount() {
      return holder.getFieldCount();
    }

    @Override
    public RelDataTypeField getField(String fieldName, boolean caseSensitive) {
      return holder.getField(typeFactory, fieldName);
    }

    @Override
    public List<String> getFieldNames() {
      return holder.getFieldNames();
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