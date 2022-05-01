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

package org.apache.drill.exec.store.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.http.providedSchema.HttpField;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSchemaBuilderForProvidedSchema {

  @Test
  public void testSimpleSchema() {
    List<HttpField> fields = generateFieldList();

    HttpJsonOptions jsonOptions = new HttpJsonOptions.HttpJsonOptionsBuilder()
      .providedSchema(fields)
      .build();

    TupleMetadata schema = jsonOptions.buildSchema();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("bigint_col", MinorType.BIGINT)
      .addNullable("boolean_col", MinorType.BIT)
      .addNullable("date_col", MinorType.DATE)
      .addNullable("double_col", MinorType.FLOAT8)
      .addNullable("interval_col", MinorType.INTERVAL)
      .addNullable("int_col", MinorType.BIGINT)
      .addNullable("timestamp_col", MinorType.TIMESTAMP)
      .addNullable("time_col", MinorType.TIME)
      .addNullable("varchar_col", MinorType.VARCHAR)
      .build();
    assertTrue(expectedSchema.isEquivalent(schema));
  }

  @Test
  public void testSingleMapSchema() {
    List<HttpField> outer = new ArrayList<>();
    List<HttpField> innerFields = generateFieldList();
    outer.add(HttpField.builder().fieldName("outer_map").fieldType("map").fields(innerFields).build());

    HttpJsonOptions jsonOptions = new HttpJsonOptions.HttpJsonOptionsBuilder()
      .providedSchema(outer)
      .build();

    TupleMetadata schema = jsonOptions.buildSchema();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addMap("outer_map")
        .addNullable("bigint_col", MinorType.BIGINT)
        .addNullable("boolean_col", MinorType.BIT)
        .addNullable("date_col", MinorType.DATE)
        .addNullable("double_col", MinorType.FLOAT8)
        .addNullable("interval_col", MinorType.INTERVAL)
        .addNullable("int_col", MinorType.BIGINT)
        .addNullable("timestamp_col", MinorType.TIMESTAMP)
        .addNullable("time_col", MinorType.TIME)
        .addNullable("varchar_col", MinorType.VARCHAR)
      .resumeSchema()
      .build();

    assertTrue(expectedSchema.isEquivalent(schema));
  }

  @Test
  public void testNestedMapSchema() {
    List<HttpField> outer = new ArrayList<>();
    List<HttpField> middle = generateFieldList();

    List<HttpField> innerFields = generateFieldList();
    middle.add(HttpField.builder().fieldName("inner_map").fieldType("map").fields(innerFields).build());
    outer.add(HttpField.builder().fieldName("outer_map").fieldType("map").fields(middle).build());

    HttpJsonOptions jsonOptions = new HttpJsonOptions.HttpJsonOptionsBuilder()
      .providedSchema(outer)
      .build();

    TupleMetadata schema = jsonOptions.buildSchema();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addMap("outer_map")
        .addNullable("bigint_col", MinorType.BIGINT)
        .addNullable("boolean_col", MinorType.BIT)
        .addNullable("date_col", MinorType.DATE)
        .addNullable("double_col", MinorType.FLOAT8)
        .addNullable("interval_col", MinorType.INTERVAL)
        .addNullable("int_col", MinorType.BIGINT)
        .addNullable("timestamp_col", MinorType.TIMESTAMP)
        .addNullable("time_col", MinorType.TIME)
        .addNullable("varchar_col", MinorType.VARCHAR)
        .addMap("inner_map")
          .addNullable("bigint_col", MinorType.BIGINT)
          .addNullable("boolean_col", MinorType.BIT)
          .addNullable("date_col", MinorType.DATE)
          .addNullable("double_col", MinorType.FLOAT8)
          .addNullable("interval_col", MinorType.INTERVAL)
          .addNullable("int_col", MinorType.BIGINT)
          .addNullable("timestamp_col", MinorType.TIMESTAMP)
          .addNullable("time_col", MinorType.TIME)
          .addNullable("varchar_col", MinorType.VARCHAR)
        .resumeMap()
      .resumeSchema()
      .build();
    assertTrue(expectedSchema.isEquivalent(schema));
  }

  @Test
  public void testSchemaWithSimpleList() {
    List<HttpField> schemaWithArray = new ArrayList<>();
    schemaWithArray.add(new HttpField.HttpFieldBuilder().fieldName("int_field").fieldType("int").build());
    schemaWithArray.add(new HttpField.HttpFieldBuilder().fieldName("int_array").fieldType("int").isArray(true).build());

    HttpJsonOptions jsonOptions = new HttpJsonOptions.HttpJsonOptionsBuilder()
      .providedSchema(schemaWithArray)
      .build();

    TupleMetadata schema = jsonOptions.buildSchema();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("int_field", MinorType.BIGINT)
      .addArray("int_array", MinorType.BIGINT)
      .build();
    assertTrue(expectedSchema.isEquivalent(schema));
  }

  @Test
  public void testListInMap() {
    List<HttpField> innerFields = new ArrayList<>();
    innerFields.add(HttpField.builder().fieldName("int_field").fieldType("int").build());
    innerFields.add(HttpField.builder().fieldName("int_array").fieldType("int").isArray(true).build());

    List<HttpField> outer = new ArrayList<>();
    outer.add(HttpField.builder().fieldName("outer_map").fieldType("map").fields(innerFields).build());

    HttpJsonOptions jsonOptions = new HttpJsonOptions.HttpJsonOptionsBuilder()
      .providedSchema(outer)
      .build();

    TupleMetadata schema = jsonOptions.buildSchema();
    TupleMetadata expectedSchema = new SchemaBuilder()
      .addMap("outer_map")
        .addNullable("int_field", MinorType.BIGINT)
        .addArray("int_array", MinorType.BIGINT)
      .resumeSchema()
      .build();

    assertTrue(expectedSchema.isEquivalent(schema));
  }

  @Test
  public void testArrayOfMaps() {
    List<HttpField> outer = new ArrayList<>();
    List<HttpField> innerFields = generateFieldList();
    outer.add(HttpField.builder().fieldName("outer_map").fieldType("map").fields(innerFields).isArray(true).build());

    HttpJsonOptions jsonOptions = new HttpJsonOptions.HttpJsonOptionsBuilder()
      .providedSchema(outer)
      .build();

    TupleMetadata schema = jsonOptions.buildSchema();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addMapArray("outer_map")
        .addNullable("bigint_col", MinorType.BIGINT)
        .addNullable("boolean_col", MinorType.BIT)
        .addNullable("date_col", MinorType.DATE)
        .addNullable("double_col", MinorType.FLOAT8)
        .addNullable("interval_col", MinorType.INTERVAL)
        .addNullable("int_col", MinorType.BIGINT)
        .addNullable("timestamp_col", MinorType.TIMESTAMP)
        .addNullable("time_col", MinorType.TIME)
        .addNullable("varchar_col", MinorType.VARCHAR)
      .resumeSchema()
      .build();

    assertTrue(expectedSchema.isEquivalent(schema));
  }

  @Test
  public void testProvidedSchemaFromJsonString() {
    String jsonString = "{\n" + "" +
      "\"type\":\"tuple_schema\",\n" +
      "    \"columns\":[\n" +
      "      {\n" +
      "        \"name\":\"outer_map\",\n" +
      "        \"type\":\"STRUCT" +
      "<`bigint_col` BIGINT, `boolean_col` BOOLEAN, `date_col` DATE, `double_col` DOUBLE, `interval_col` INTERVAL, `int_col` BIGINT, `timestamp_col` TIMESTAMP, `time_col` TIME, `varchar_col` VARCHAR, `inner_map` STRUCT<`bigint_col` BIGINT, `boolean_col` BOOLEAN, `date_col` DATE, `double_col` DOUBLE, `interval_col` INTERVAL, `int_col` BIGINT, `timestamp_col` TIMESTAMP, `time_col` TIME, `varchar_col` VARCHAR>>\",\n" +
      "        \"mode\":\"REQUIRED\"\n" +
      "      }\n" +
      "    ]\n" +
      "  }";

    HttpJsonOptions jsonOptions = new HttpJsonOptions.HttpJsonOptionsBuilder()
      .jsonSchema(jsonString)
      .build();

    TupleMetadata schema = null;
    try {
      schema = jsonOptions.getSchemaFromJsonString();
    } catch (JsonProcessingException e) {
      fail();
    }

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addMap("outer_map")
      .addNullable("bigint_col", MinorType.BIGINT)
      .addNullable("boolean_col", MinorType.BIT)
      .addNullable("date_col", MinorType.DATE)
      .addNullable("double_col", MinorType.FLOAT8)
      .addNullable("interval_col", MinorType.INTERVAL)
      .addNullable("int_col", MinorType.BIGINT)
      .addNullable("timestamp_col", MinorType.TIMESTAMP)
      .addNullable("time_col", MinorType.TIME)
      .addNullable("varchar_col", MinorType.VARCHAR)
        .addMap("inner_map")
          .addNullable("bigint_col", MinorType.BIGINT)
          .addNullable("boolean_col", MinorType.BIT)
          .addNullable("date_col", MinorType.DATE)
          .addNullable("double_col", MinorType.FLOAT8)
          .addNullable("interval_col", MinorType.INTERVAL)
          .addNullable("int_col", MinorType.BIGINT)
          .addNullable("timestamp_col", MinorType.TIMESTAMP)
          .addNullable("time_col", MinorType.TIME)
          .addNullable("varchar_col", MinorType.VARCHAR)
        .resumeMap()
      .resumeSchema()
      .build();

    assertTrue(expectedSchema.isEquivalent(schema));
  }

  private List<HttpField> generateFieldList() {
    List<HttpField> fields = new ArrayList<>();
    fields.add(HttpField.builder().fieldName("bigint_col").fieldType("bigint").build());
    fields.add(HttpField.builder().fieldName("boolean_col").fieldType("boolean").build());
    fields.add(HttpField.builder().fieldName("date_col").fieldType("date").build());
    fields.add(HttpField.builder().fieldName("double_col").fieldType("double").build());
    fields.add(HttpField.builder().fieldName("interval_col").fieldType("interval").build());
    fields.add(HttpField.builder().fieldName("int_col").fieldType("int").build());
    fields.add(HttpField.builder().fieldName("timestamp_col").fieldType("timestamp").build());
    fields.add(HttpField.builder().fieldName("time_col").fieldType("time").build());
    fields.add(HttpField.builder().fieldName("varchar_col").fieldType("varchar").build());

    return fields;
  }
}
