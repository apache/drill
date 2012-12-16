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

public class SchemaRecorder {

    /*
    public SchemaRecorder() {

    }

    public RecordSchema getCurrentSchema() {
        return currentSchema;
    }

    public void recordData() throws IOException {

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

    }
    */
}
