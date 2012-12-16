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

package org.apache.drill.common.physical.schema.transform;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.protobuf.DescriptorProtos;
import org.apache.drill.common.physical.schema.Field;
import org.apache.drill.common.physical.schema.ListSchema;
import org.apache.drill.common.physical.schema.ObjectSchema;
import org.apache.drill.common.physical.schema.RecordSchema;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ProtobufSchemaTransformer implements SchemaTransformer<DescriptorProtos.DescriptorProto> {
    private static final Map<Field.FieldType, Function<Field, Object>> FIELD_MAP = Maps.newEnumMap(Field.FieldType.class);
    private static final Map<Field.FieldType, DescriptorProtos.FieldDescriptorProto.Type> TYPE_MAP = Maps.newEnumMap(Field.FieldType.class);
    private int fieldIndex = 0;
    public static final String LIST_SCHEMA_NAME = "_EmbeddedList"; //Read from config?

    static {
        TYPE_MAP.put(Field.FieldType.BOOLEAN, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL);
        TYPE_MAP.put(Field.FieldType.STRING, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
        TYPE_MAP.put(Field.FieldType.FLOAT, DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT);
        TYPE_MAP.put(Field.FieldType.INTEGER, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
    }

    private DescriptorProtos.DescriptorProto.Builder transformSchema(String name, DescriptorProtos.DescriptorProto.Builder parentBuilder, RecordSchema schema) {
        if (schema instanceof ObjectSchema) {
            return addObjectSchema(name, parentBuilder, ObjectSchema.class.cast(schema));
        } else if (schema instanceof ListSchema) {
            return addListSchema(name, ListSchema.class.cast(schema));
        } else {
            throw new RuntimeException("Unknown schema passed to transformer: " + schema);
        }
    }

    public DescriptorProtos.DescriptorProto transformSchema(String name, RecordSchema schema) {
        return transformSchema(name, null, schema).build();
    }

    private DescriptorProtos.DescriptorProto.Builder addListSchema(String name, ListSchema schema) {
        DescriptorProtos.DescriptorProto.Builder builder = DescriptorProtos.DescriptorProto.newBuilder().setName(name);
        DescriptorProtos.FieldDescriptorProto.Builder builderForValue = DescriptorProtos.FieldDescriptorProto.newBuilder();
        builderForValue.setTypeName(LIST_SCHEMA_NAME).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE);
        builder.addField(builderForValue);
        return builder;
    }

    private DescriptorProtos.DescriptorProto.Builder addObjectSchema(String name,
                                                                     DescriptorProtos.DescriptorProto.Builder parentBuilder,
                                                                     ObjectSchema schema) {
        DescriptorProtos.DescriptorProto.Builder builder = DescriptorProtos.DescriptorProto.newBuilder().setName(name);
        for (Field field : schema.getFields()) {
            DescriptorProtos.FieldDescriptorProto.Builder builderForValue = DescriptorProtos.FieldDescriptorProto.newBuilder();
            String fieldName = field.getFieldName();
            builderForValue.setName(fieldName).setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL).setNumber(++fieldIndex);
            if (field.hasSchema()) {
                RecordSchema innerSchema = field.getAssignedSchema();
                if (innerSchema instanceof ObjectSchema) {
                    addObjectSchema(fieldName, builder, (ObjectSchema) innerSchema);
                    DescriptorProtos.DescriptorProto innerProto = Iterables.getLast(builder.getNestedTypeList());
                    builderForValue.setTypeName(innerProto.getName()).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE);
                } else if (innerSchema instanceof ListSchema) {
                    builderForValue.setTypeName(LIST_SCHEMA_NAME).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE);
                }
            } else {
                builderForValue.setType(getProtoType(field.getFieldType()));
            }
            builder.addField(builderForValue);
        }

        if (parentBuilder != null) {
            parentBuilder.addNestedType(builder);
        }

        return builder;
    }

    private DescriptorProtos.FieldDescriptorProto.Type getProtoType(Field.FieldType fieldType) {
        return checkNotNull(TYPE_MAP.get(fieldType));
    }
}
