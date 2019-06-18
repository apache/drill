/*
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
package org.apache.drill.metastore.iceberg.schema;

import com.sun.codemodel.CodeWriter;
import com.sun.codemodel.JAnnotationArrayMember;
import com.sun.codemodel.JAnnotationUse;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JPackage;
import net.openhft.compiler.CompilerUtils;
import org.apache.drill.metastore.MetastoreFieldDefinition;
import org.apache.drill.metastore.iceberg.IcebergBaseTest;
import org.apache.drill.metastore.iceberg.exceptions.IcebergMetastoreException;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestIcebergTableSchema extends IcebergBaseTest {

  @Test
  public void testAllTypes() throws Exception {
    Class<?> clazz = new ClassGenerator(getClass().getSimpleName() + "AllTypes") {

      @Override
      void addFields(JDefinedClass jDefinedClass) {
        JFieldVar stringField = jDefinedClass.field(DEFAULT_FIELD_MODE, String.class, "stringField");
        annotate(stringField);

        JFieldVar intField = jDefinedClass.field(DEFAULT_FIELD_MODE, int.class, "intField");
        annotate(intField);

        JFieldVar integerField = jDefinedClass.field(DEFAULT_FIELD_MODE, Integer.class, "integerField");
        annotate(integerField);

        JFieldVar longField = jDefinedClass.field(DEFAULT_FIELD_MODE, Long.class, "longField");
        annotate(longField);

        JFieldVar floatField = jDefinedClass.field(DEFAULT_FIELD_MODE, Float.class, "floatField");
        annotate(floatField);

        JFieldVar doubleField = jDefinedClass.field(DEFAULT_FIELD_MODE, Double.class, "doubleField");
        annotate(doubleField);

        JFieldVar booleanField = jDefinedClass.field(DEFAULT_FIELD_MODE, Boolean.class, "booleanField");
        annotate(booleanField);

        JCodeModel jCodeModel = jDefinedClass.owner();

        JClass listRef = jCodeModel.ref(List.class).narrow(String.class);
        JFieldVar listField = jDefinedClass.field(DEFAULT_FIELD_MODE, listRef, "listField");
        annotate(listField);

        JClass mapRef = jCodeModel.ref(Map.class).narrow(String.class, Float.class);
        JFieldVar mapField = jDefinedClass.field(DEFAULT_FIELD_MODE, mapRef, "mapField");
        annotate(mapField);
      }

    }.generate();

    IcebergTableSchema schema = IcebergTableSchema.of(clazz, Collections.emptyList());

    int schemaIndex = IcebergTableSchema.STARTING_SCHEMA_INDEX;
    int complexTypesIndex = IcebergTableSchema.STARTING_COMPLEX_TYPES_INDEX;

    Schema expectedSchema = new Schema(
      Types.NestedField.optional(schemaIndex++, "stringField", Types.StringType.get()),
      Types.NestedField.optional(schemaIndex++, "intField", Types.IntegerType.get()),
      Types.NestedField.optional(schemaIndex++, "integerField", Types.IntegerType.get()),
      Types.NestedField.optional(schemaIndex++, "longField", Types.LongType.get()),
      Types.NestedField.optional(schemaIndex++, "floatField", Types.FloatType.get()),
      Types.NestedField.optional(schemaIndex++, "doubleField", Types.DoubleType.get()),
      Types.NestedField.optional(schemaIndex++, "booleanField", Types.BooleanType.get()),
      Types.NestedField.optional(schemaIndex++, "listField",
        Types.ListType.ofOptional(complexTypesIndex++, Types.StringType.get())),
      Types.NestedField.optional(schemaIndex, "mapField",
        Types.MapType.ofOptional(complexTypesIndex++, complexTypesIndex, Types.StringType.get(), Types.FloatType.get())));

    assertEquals(expectedSchema.asStruct(), schema.tableSchema().asStruct());
  }

  @Test
  public void testIgnoreUnannotatedFields() throws Exception {
    Class<?> clazz = new ClassGenerator(getClass().getSimpleName() + "IgnoreUnannotatedFields") {

      @Override
      void addFields(JDefinedClass jDefinedClass) {
        JFieldVar stringField = jDefinedClass.field(DEFAULT_FIELD_MODE, String.class, "stringField");
        annotate(stringField);

        jDefinedClass.field(DEFAULT_FIELD_MODE, Integer.class, "integerField");
      }
    }.generate();

    IcebergTableSchema schema = IcebergTableSchema.of(clazz, Collections.emptyList());
    assertNotNull(schema.tableSchema().findField("stringField"));
    assertNull(schema.tableSchema().findField("integerField"));
  }

  @Test
  public void testNestedComplexType() throws Exception {
    Class<?> clazz = new ClassGenerator(getClass().getSimpleName() + "NestedComplexType") {

      @Override
      void addFields(JDefinedClass jDefinedClass) {
        JCodeModel jCodeModel = jDefinedClass.owner();

        JClass nestedListRef = jCodeModel.ref(List.class).narrow(String.class);
        JClass listRef = jCodeModel.ref(List.class).narrow(nestedListRef);
        JFieldVar listField = jDefinedClass.field(DEFAULT_FIELD_MODE, listRef, "listField");
        annotate(listField);
      }
    }.generate();

    thrown.expect(IcebergMetastoreException.class);

    IcebergTableSchema.of(clazz, Collections.emptyList());
  }

  @Test
  public void testUnpartitionedPartitionSpec() throws Exception {
    Class<?> clazz = new ClassGenerator(getClass().getSimpleName() + "UnpartitionedPartitionSpec") {

      @Override
      void addFields(JDefinedClass jDefinedClass) {
        JFieldVar stringField = jDefinedClass.field(DEFAULT_FIELD_MODE, String.class, "stringField");
        annotate(stringField);
      }
    }.generate();

    IcebergTableSchema schema = IcebergTableSchema.of(clazz, Collections.emptyList());
    assertNotNull(schema.tableSchema().findField("stringField"));

    assertEquals(PartitionSpec.unpartitioned(), schema.partitionSpec());
  }

  @Test
  public void testPartitionedPartitionSpec() throws Exception {
    Class<?> clazz = new ClassGenerator(getClass().getSimpleName() + "PartitionedPartitionSpec") {

      @Override
      void addFields(JDefinedClass jDefinedClass) {
        JFieldVar partKey1 = jDefinedClass.field(DEFAULT_FIELD_MODE, String.class, "partKey1");
        annotate(partKey1);

        JFieldVar partKey2 = jDefinedClass.field(DEFAULT_FIELD_MODE, String.class, "partKey2");
        annotate(partKey2);

        JFieldVar partKey3 = jDefinedClass.field(DEFAULT_FIELD_MODE, String.class, "partKey3");
        annotate(partKey3);

        JFieldVar integerField = jDefinedClass.field(DEFAULT_FIELD_MODE, Integer.class, "integerField");
        annotate(integerField);

        JFieldVar booleanField = jDefinedClass.field(DEFAULT_FIELD_MODE, Boolean.class, "booleanField");
        annotate(booleanField);
      }
    }.generate();

    IcebergTableSchema schema = IcebergTableSchema.of(clazz, Arrays.asList("partKey1", "partKey2", "partKey3"));

    Types.NestedField partKey1 = schema.tableSchema().findField("partKey1");
    assertNotNull(partKey1);

    Types.NestedField partKey2 = schema.tableSchema().findField("partKey2");
    assertNotNull(partKey2);

    Types.NestedField partKey3 = schema.tableSchema().findField("partKey3");
    assertNotNull(partKey3);

    assertNotNull(schema.tableSchema().findField("integerField"));
    assertNotNull(schema.tableSchema().findField("booleanField"));

    Schema partitionSchema = new Schema(partKey1, partKey2, partKey3);
    PartitionSpec expectedPartitionSpec = PartitionSpec.builderFor(partitionSchema)
      .identity(partKey1.name())
      .identity(partKey2.name())
      .identity(partKey3.name())
      .build();

    assertEquals(expectedPartitionSpec, schema.partitionSpec());
  }

  @Test
  public void testUnMatchingPartitionSpec() throws Exception {
    Class<?> clazz = new ClassGenerator(getClass().getSimpleName() + "UnMatchingPartitionSpec") {

      @Override
      void addFields(JDefinedClass jDefinedClass) {
        JFieldVar partKey1 = jDefinedClass.field(DEFAULT_FIELD_MODE, String.class, "partKey1");
        annotate(partKey1);

        JFieldVar integerField = jDefinedClass.field(DEFAULT_FIELD_MODE, Integer.class, "integerField");
        annotate(integerField);
      }
    }.generate();

    thrown.expect(IcebergMetastoreException.class);

    IcebergTableSchema.of(clazz, Arrays.asList("partKey1", "partKey2"));
  }

  /**
   * Generates and loads class at the runtime with specified fields.
   * Fields may or may not be annotated.
   */
  private abstract class ClassGenerator {

    final int DEFAULT_FIELD_MODE = JMod.PRIVATE;

    private final String name;

    ClassGenerator(String name) {
      this.name = name;
    }

    Class<?> generate() throws JClassAlreadyExistsException, IOException, ClassNotFoundException {
      JCodeModel jCodeModel = prepareModel();
      ByteArrayStreamCodeWriter codeWriter = new ByteArrayStreamCodeWriter();
      jCodeModel.build(codeWriter);

      String sourceCode = codeWriter.sourceCode();
      return CompilerUtils.CACHED_COMPILER.loadFromJava(name, sourceCode);
    }

    private JCodeModel prepareModel() throws JClassAlreadyExistsException {
      JCodeModel jCodeModel = new JCodeModel();
      JPackage jPackage = jCodeModel._package("");
      JDefinedClass jDefinedClass = jPackage._class(name);
      addFields(jDefinedClass);
      return jCodeModel;
    }

    void annotate(JFieldVar field) {
      annotate(field, MetadataType.ALL);
    }

    void annotate(JFieldVar field, MetadataType... scopes) {
      JAnnotationUse annotate = field.annotate(MetastoreFieldDefinition.class);
      assert scopes.length != 0;
      JAnnotationArrayMember scopesParam = annotate.paramArray("scopes");
      Stream.of(scopes).forEach(scopesParam::param);
    }

    abstract void addFields(JDefinedClass jDefinedClass);

    private class ByteArrayStreamCodeWriter extends CodeWriter {

      private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

      @Override
      public OutputStream openBinary(JPackage pkg, String fileName) {
        return outputStream;
      }

      @Override
      public void close() {
        // no need to close byte array stream
      }

      String sourceCode() {
        return new String(outputStream.toByteArray());
      }
    }
  }
}
