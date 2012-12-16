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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.apache.drill.common.physical.schema.*;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;

public class ProtobufSchemaTransformerTest {
    ProtobufSchemaTransformer transformer;

    @Before
    public void setUp() {
        transformer = new ProtobufSchemaTransformer();
    }

    @Test
    public void testTransformObjectSchema() throws Exception {
        ObjectSchema schema = new ObjectSchema();
        SchemaIdGenerator generator = new SchemaIdGenerator();
        schema.addField(new NamedField(null, generator, "", "s", Field.FieldType.STRING));
        schema.addField(new NamedField(null, generator, "", "i", Field.FieldType.INTEGER));
        DescriptorProtos.DescriptorProto proto = transformer.transformSchema("TestMessage", schema);
        assertEquals("TestMessage", proto.getName());
        FieldsVerifier fields = new FieldsVerifier();
        fields.addField("s", new FieldData(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        fields.addField("i", new FieldData(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32));
        fields.verifyFields(proto);
    }

    @Test
    public void testTransformNestedObjectSchema() throws Exception {
        ObjectSchema schema = new ObjectSchema();
        SchemaIdGenerator generator = new SchemaIdGenerator();
        schema.addField(new NamedField(schema, generator, "", "s", Field.FieldType.STRING));
        schema.addField(new NamedField(schema, generator, "", "i", Field.FieldType.INTEGER));
        ObjectSchema childSchema = new ObjectSchema();
        childSchema.addField(new NamedField(childSchema, generator, "child", "s", Field.FieldType.STRING));
        childSchema.addField(new NamedField(childSchema, generator, "child", "i", Field.FieldType.INTEGER));
        schema.addField(new NamedField(schema, generator, "", "innerMap", Field.FieldType.MAP).assignSchema(childSchema));
        DescriptorProtos.DescriptorProto proto = transformer.transformSchema("TestMessage", schema);
        assertEquals("TestMessage", proto.getName());
        FieldsVerifier fields = new FieldsVerifier();
        fields.addField("s", new FieldData(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        fields.addField("i", new FieldData(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32));
        FieldData mapData = new FieldData(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE);
        FieldsVerifier innerFields = new FieldsVerifier();
        innerFields.addField("s", new FieldData(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING));
        innerFields.addField("i", new FieldData(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32));
        mapData.assignVerifier(innerFields);
        fields.addField("innerMap", mapData);
        fields.verifyFields(proto);
    }

    @Test
    public void testTransformListSchema() throws Exception {
        SchemaIdGenerator idGenerator = new SchemaIdGenerator();
        ListSchema listSchema = new ListSchema();
        NamedField field = new NamedField(null, idGenerator, "a", "a", Field.FieldType.INTEGER);
        listSchema.addField(field);
        DescriptorProtos.DescriptorProto proto = transformer.transformSchema("l", listSchema);
        assertEquals("l", proto.getName());
        assertEquals(1, proto.getFieldCount());
        assertEquals(ProtobufSchemaTransformer.LIST_SCHEMA_NAME, proto.getField(0).getTypeName());
        assertEquals(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, proto.getField(0).getType());
    }

    @Test
    public void testTransformNestedListSchema() throws Exception {
        SchemaIdGenerator idGenerator = new SchemaIdGenerator();
        ListSchema listSchema = new ListSchema();
        NamedField field = new NamedField(null, idGenerator, "a", "a", Field.FieldType.ARRAY);
        listSchema.addField(field);
        ListSchema childListSchema = new ListSchema();
        NamedField childField = new NamedField(listSchema, idGenerator, "b", "b", Field.FieldType.ARRAY);
        childListSchema.addField(childField);
        field.assignSchema(childListSchema);
        transformer.transformSchema("l", listSchema);
        DescriptorProtos.DescriptorProto proto = transformer.transformSchema("l", listSchema);
        assertEquals("l", proto.getName());
        FieldsVerifier verifier = new FieldsVerifier();
        verifier.addField("a", new FieldData(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE));
        assertEquals(1, proto.getFieldCount());
        assertEquals(ProtobufSchemaTransformer.LIST_SCHEMA_NAME, proto.getField(0).getTypeName());
        assertEquals(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, proto.getField(0).getType());
    }

    @Test
    public void testTransformParentObjectAndListSchema() throws Exception {
        SchemaIdGenerator idGenerator = new SchemaIdGenerator();
        ObjectSchema objectSchema = new ObjectSchema();
        NamedField field = new NamedField(null, idGenerator, "a", "a", Field.FieldType.INTEGER);
        objectSchema.addField(field);
        ListSchema listSchema = new ListSchema();
        NamedField listField = new NamedField(null, idGenerator, "b", "b", Field.FieldType.ARRAY);
        objectSchema.addField(listField);
        listSchema.addField(new NamedField(objectSchema, idGenerator, "aa", "ab", Field.FieldType.STRING));
        listField.assignSchema(listSchema);
        DescriptorProtos.DescriptorProto proto = transformer.transformSchema("objectWithList", objectSchema);
        assertEquals(2, proto.getFieldCount());
        FieldsVerifier verifier = new FieldsVerifier();
        verifier.addField("a", new FieldData(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32));
        FieldData arrayField = new FieldData(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE);
        verifier.addField("b", arrayField);
        FieldsVerifier listVerifier = new FieldsVerifier();
        listVerifier.addField(ProtobufSchemaTransformer.LIST_SCHEMA_NAME, new FieldData(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE));
        arrayField.assignVerifier(listVerifier);
    }

    private static class FieldsVerifier {
        Map<String, FieldData> fields;

        public FieldsVerifier() {
            fields = Maps.newHashMap();
        }

        public void addField(String fieldName, FieldData data) {
            fields.put(fieldName, data);
        }

        public FieldData getField(String fieldName) {
            FieldData data = fields.get(fieldName);
            assertNotNull(data);
            return data;
        }

        public void verifyFields(DescriptorProtos.DescriptorProto proto) {
            List<DescriptorProtos.FieldDescriptorProto> foundFields = proto.getFieldList();
            assertEquals(fields.size(), foundFields.size());
            for (final DescriptorProtos.FieldDescriptorProto fieldProto : foundFields) {
                final FieldData data = fields.get(fieldProto.getName());
                assertNotNull(data);
                assertEquals(data.type, fieldProto.getType());
                if (data.type == DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE) {
                    assertNotNull(data.innerFields);
                    DescriptorProtos.DescriptorProto innerType = Iterables.find(proto.getNestedTypeList(), new Predicate<DescriptorProtos.DescriptorProto>() {
                        @Override
                        public boolean apply(DescriptorProtos.DescriptorProto descriptorProto) {
                            return descriptorProto.getName() == fieldProto.getTypeName();
                        }
                    });

                    data.innerFields.verifyFields(innerType);
                }
                data.isRead = true;
            }

            assertFalse(Iterables.any(fields.values(), new Predicate<FieldData>() {
                @Override
                public boolean apply(FieldData fieldData) {
                    return fieldData.isRead == false;
                }
            }));
        }
    }

    private static class FieldData {
        DescriptorProtos.FieldDescriptorProto.Type type;
        FieldsVerifier innerFields;
        boolean isRead = false;

        private FieldData(DescriptorProtos.FieldDescriptorProto.Type type) {
            this.type = type;
        }

        public void assignVerifier(FieldsVerifier innerFields) {
            this.innerFields = innerFields;
        }
    }
}
