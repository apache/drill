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

import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.drill.exec.schema.json.jackson.JacksonHelper;
import org.apache.drill.exec.schema.json.jackson.ScanJson;

import java.io.IOException;
import java.util.List;

public class SchemaRecorder {
    DiffSchema diffSchema;
    RecordSchema currentSchema;
    List<Field> removedFields;

    public SchemaRecorder() {
        currentSchema = new ObjectSchema();
        diffSchema = new DiffSchema();
        removedFields = Lists.newArrayList();
    }

    public RecordSchema getCurrentSchema() {
        return currentSchema;
    }

    public void recordData(ScanJson.ReadType currentReadType, ScanJson.ReadType readType, JsonParser parser, IdGenerator generator, DataRecord record, Field.FieldType fieldType, String prefixFieldName, String fieldName, int index) throws IOException {
        Field field = currentSchema.getField(fieldName, index);

        if (field == null || field.getFieldType() != fieldType) {
            if (field != null) {
                removeStaleField(index, field);
            }
            field = currentReadType.createField(currentSchema, generator, prefixFieldName, fieldName, fieldType, index);
            field.setRead(true);
            diffSchema.recordNewField(field);
            currentSchema.addField(field);
        } else {
            field.setRead(true);
        }

        if (readType != null) {
            RecordSchema origSchema = currentSchema;
            if (field != null) {
                currentSchema = field.getAssignedSchema();
            }

            RecordSchema newSchema = readType.createSchema();
            field.assignSchemaIfNull(newSchema);
            setCurrentSchemaIfNull(newSchema);
            readType.readRecord(parser, generator, this, record, field.getFullFieldName());

            currentSchema = origSchema;
        } else {
            RecordSchema schema = field.getParentSchema();
            record.addData(field.getFieldId(), JacksonHelper.getValueFromFieldType(parser, fieldType), schema != null && schema instanceof ListSchema);
        }
    }

    private void removeStaleField(int index, Field field) {
        if (field.hasSchema()) {
            removeChildFields(field);
        }
        removedFields.add(field);
        currentSchema.removeField(field, index);
    }

    private void removeChildFields(Field field) {
        RecordSchema schema = field.getAssignedSchema();
        if(schema == null) { return; }
        for (Field childField : schema.getFields()) {
            removedFields.add(childField);
            if (childField.hasSchema()) {
                removeChildFields(childField);
            }
        }
    }

    public boolean hasDiffs() {
        return diffSchema.hasDiffFields();
    }

    public DiffSchema getDiffSchema() {
        return hasDiffs() ? diffSchema : null;
    }

    public void setCurrentSchemaIfNull(RecordSchema newSchema) {
        if (currentSchema == null) {
            currentSchema = newSchema;
        }
    }

    public void reset() {
        currentSchema.resetMarkedFields();
        diffSchema.reset();
        removedFields.clear();
    }

    public void addMissingFields() {
        for (Field field : Iterables.concat(currentSchema.removeUnreadFields(), removedFields)) {
            diffSchema.addRemovedField(field);
        }
    }
}
