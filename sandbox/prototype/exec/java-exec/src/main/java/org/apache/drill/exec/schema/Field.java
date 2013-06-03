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
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.vector.*;
import org.apache.drill.exec.store.BatchExceededException;
import org.apache.drill.exec.store.VectorHolder;

import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.drill.exec.proto.SchemaDefProtos.*;

public abstract class Field {
    final MajorType fieldType;
    final int parentFieldId;
    final int fieldId;
    final String prefixFieldName;
    RecordSchema schema;
    RecordSchema parentSchema;
    boolean read;

    public Field(RecordSchema parentSchema, int parentFieldId, IdGenerator<Integer> generator, MajorType type, String prefixFieldName) {
        this.fieldId = generator.getNextId();
        fieldType = type;
        this.prefixFieldName = prefixFieldName;
        this.parentSchema = parentSchema;
        this.parentFieldId = parentFieldId;
    }

    public abstract String getFieldName();

    public String getFullFieldName() {
        return Strings.isNullOrEmpty(prefixFieldName) ? getFieldName() : prefixFieldName + "." + getFieldName();
    }

    public int getFieldId() {
        return fieldId;
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

    public RecordSchema getAssignedSchema() {
        return schema;
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

    public MajorType getFieldType() {
        return fieldType;
    }
}
