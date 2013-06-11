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

package org.apache.drill.exec.schema;

import com.google.common.base.Objects;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.schema.json.jackson.JacksonHelper;

public class NamedField extends Field {
    final SchemaDefProtos.MajorType keyType;
    String fieldName;

    public NamedField(RecordSchema parentSchema, int parentFieldId, IdGenerator<Integer> generator, String prefixFieldName, String fieldName, SchemaDefProtos.MajorType fieldType) {
        this(parentSchema, parentFieldId, generator, prefixFieldName, fieldName, fieldType, JacksonHelper.STRING_TYPE);
    }

    public NamedField(RecordSchema parentSchema,
                      int parentFieldId,
                      IdGenerator<Integer> generator,
                      String prefixFieldName,
                      String fieldName,
                      SchemaDefProtos.MajorType fieldType,
                      SchemaDefProtos.MajorType keyType) {
        super(parentSchema, parentFieldId, generator, fieldType, prefixFieldName);
        this.fieldName = fieldName;
        this.keyType = keyType;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    protected Objects.ToStringHelper addAttributesToHelper(Objects.ToStringHelper helper) {
        return helper.add("keyType", keyType);
    }
}
