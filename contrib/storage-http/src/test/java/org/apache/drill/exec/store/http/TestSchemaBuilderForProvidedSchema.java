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

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.http.providedSchema.HttpField;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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
    outer.add(new HttpField("outer_map", "map", null, innerFields));

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
    middle.add(new HttpField("inner_map", "map", null, innerFields));
    outer.add(new HttpField("outer_map", "map", null, middle));

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


  private List<HttpField> generateFieldList() {
    List<HttpField> fields = new ArrayList<>();
    fields.add(new HttpField("bigint_col", "bigint"));
    fields.add(new HttpField("boolean_col", "boolean"));
    fields.add(new HttpField("date_col", "date"));
    fields.add(new HttpField("double_col", "double"));
    fields.add(new HttpField("interval_col", "interval"));
    fields.add(new HttpField("int_col", "int"));
    fields.add(new HttpField("timestamp_col", "timestamp"));
    fields.add(new HttpField("time_col", "time"));
    fields.add(new HttpField("varchar_col", "varchar"));

    return fields;
  }


}
