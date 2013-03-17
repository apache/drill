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
import com.google.common.base.Strings;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public abstract class Field {
    final FieldType fieldType;
    int fieldId;
    String prefixFieldName;
    String fieldName;
    RecordSchema schema;
    RecordSchema parentSchema;
    boolean read;


    public Field(RecordSchema parentSchema, IdGenerator<Integer> generator, FieldType fieldType, String prefixFieldName) {
        this.fieldId = generator.getNextId();
        this.fieldType = fieldType;
        this.prefixFieldName = prefixFieldName;
        this.parentSchema = parentSchema;
    }

    public Field assignSchema(RecordSchema newSchema) {
        checkState(schema == null, "Schema already assigned to field: %s", fieldName);
        checkState(fieldType.isEmbedSchema(), "Schema cannot be assigned to non-embedded types: %s", fieldType);
        schema = newSchema;
        return this;
    }

    public String getFullFieldName() {
        return Strings.isNullOrEmpty(prefixFieldName) ? fieldName : prefixFieldName + "." + fieldName;
    }

    public int getFieldId() {
        return fieldId;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setRead(boolean read) {
        this.read = read;
    }

    protected abstract Objects.ToStringHelper addAttributesToHelper(Objects.ToStringHelper helper);

    Objects.ToStringHelper getAttributesStringHelper() {
        return Objects.toStringHelper(this).add("type", fieldType)
                .add("id", fieldId)
                .add("fullFieldName", getFullFieldName())
                .add("schema", schema == null ? null : schema.toSchemaString()).omitNullValues();
    }

    @Override
    public String toString() {
        return addAttributesToHelper(getAttributesStringHelper()).toString();
    }

    public RecordSchema getParentSchema() {
        return parentSchema;
    }

    public RecordSchema getAssignedSchema() {
        return schema;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public void assignSchemaIfNull(RecordSchema newSchema) {
        if (!hasSchema()) {
            schema = newSchema;
        }
    }

    public boolean isRead() {
        return read;
    }

    public boolean hasSchema() {
        return schema != null;
    }

    public static enum FieldType {
        INTEGER(1),
        FLOAT(2),
        BOOLEAN(3),
        STRING(4),
        ARRAY(5, true),
        MAP(6, true);

        byte value;
        boolean embedSchema;

        FieldType(int value, boolean embedSchema) {
            this.value = (byte) value;
            this.embedSchema = embedSchema;
        }

        FieldType(int value) {
            this(value, false);
        }

        public byte value() {
            return value;
        }

        public boolean isEmbedSchema() {
            return embedSchema;
        }
    }
}
