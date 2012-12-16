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
    final FieldType fieldType;
    final int parentFieldId;
    final int fieldId;
    final String prefixFieldName;
    RecordSchema schema;
    RecordSchema parentSchema;
    boolean read;
    private MaterializedField materializedField;


    public Field(RecordSchema parentSchema, int parentFieldId, IdGenerator<Integer> generator, FieldType fieldType, String prefixFieldName) {
        this.fieldId = generator.getNextId();
        this.fieldType = fieldType;
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

    private static MajorType buildMajorType(MinorType minorType) {
        return MajorType.newBuilder().setMinorType(minorType).setMode(DataMode.REQUIRED).build();
    }

    private static MajorType buildRepeatedMajorType(MinorType minorType) {
        return MajorType.newBuilder().setMinorType(minorType).setMode(DataMode.REPEATED).build();
    }

    public static enum FieldType {
        INTEGER(1, buildMajorType(MinorType.INT)) {
            @Override
            public <T> boolean addValueToVector(int index, VectorHolder holder, BufferAllocator allocator, T val) {
                holder.incAndCheckLength(32);
                NullableFixed4 fixed4 = (NullableFixed4) holder.getValueVector();
                if (val == null) {
                    fixed4.setNull(index);
                } else {
                    fixed4.setInt(index, (Integer) val);
                }
                return holder.hasEnoughSpace(32);
            }
        },
        FLOAT(2, buildMajorType(MinorType.FLOAT4)) {
            @Override
            public <T> boolean addValueToVector(int index, VectorHolder holder, BufferAllocator allocator, T val) {
                holder.incAndCheckLength(32);
                NullableFixed4 fixed4 = (NullableFixed4) holder.getValueVector();
                if (val == null) {
                    fixed4.setNull(index);
                } else {
                    fixed4.setFloat4(index, (Float) val);
                }
                return holder.hasEnoughSpace(32);
            }
        },
        BOOLEAN(3, buildMajorType(MinorType.BOOLEAN)) {
            @Override
            public <T> boolean addValueToVector(int index, VectorHolder holder, BufferAllocator allocator, T val) {
                holder.incAndCheckLength(1);
                Bit bit = (Bit) holder.getValueVector();
                if ((Boolean) val) {
                    bit.set(index);
                }
                return holder.hasEnoughSpace(1);
            }
        },
        STRING(4, buildMajorType(MinorType.VARCHAR4)) {
            @Override
            public <T> boolean addValueToVector(int index, VectorHolder holder, BufferAllocator allocator, T val) {
                if (val == null) {
                    ((NullableVarLen4) holder.getValueVector()).setNull(index);
                    return (index + 1) * 4 <= holder.getLength();
                } else {
                    byte[] bytes = ((String) val).getBytes(Constants.UTF8_CHARSET);
                    int length = bytes.length * 8;
                    holder.incAndCheckLength(length);
                    NullableVarLen4 varLen4 = (NullableVarLen4) holder.getValueVector();
                    varLen4.setBytes(index, bytes);
                    return holder.hasEnoughSpace(length);
                }
            }
        },
        ARRAY(5, buildRepeatedMajorType(MinorType.LATE), true) {
            @Override
            public <T> boolean addValueToVector(int index, VectorHolder holder, BufferAllocator allocator, T val) {
                throw new UnsupportedOperationException("Array type not yet supported.");
            }
        },

        MAP(6, buildMajorType(MinorType.MAP), true) {
            @Override
            public <T> boolean addValueToVector(int index, VectorHolder holder, BufferAllocator allocator, T val) {
                throw new UnsupportedOperationException("Map type not yet supported.");
            }
        };

        byte value;
        boolean embedSchema;
        MajorType majorType;

        FieldType(int value, MajorType majorType, boolean embedSchema) {
            this.value = (byte) value;
            this.embedSchema = embedSchema;
            this.majorType = majorType;
        }

        FieldType(int value, MajorType majorType) {
            this(value, majorType, false);
        }

        public byte value() {
            return value;
        }

        public boolean isEmbedSchema() {
            return embedSchema;
        }

        public MajorType toMajorType() {
            return majorType;
        }

        public abstract <T> boolean addValueToVector(int index, VectorHolder holder, BufferAllocator allocator, T val) throws BatchExceededException;

        private static class Constants {
            public static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
        }
    }
}
