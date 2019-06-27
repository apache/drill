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
package org.apache.drill.exec.record.metadata.schema.parser;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.joda.time.LocalDate;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestSchemaParser {

  @Test
  public void checkQuotedIdWithEscapes() throws Exception {
    String schemaWithEscapes = "`a\\\\b\\`c` INT";
    assertEquals(schemaWithEscapes, SchemaExprParser.parseSchema(schemaWithEscapes).metadata(0).columnString());

    String schemaWithKeywords = "`INTEGER` INT";
    assertEquals(schemaWithKeywords, SchemaExprParser.parseSchema(schemaWithKeywords).metadata(0).columnString());
  }

  @Test
  public void testSchemaWithParen() throws Exception {
    String schemaWithParen = "(`a` INT NOT NULL, `b` VARCHAR(10))";
    TupleMetadata schema = SchemaExprParser.parseSchema(schemaWithParen);
    assertEquals(2, schema.size());
    assertEquals("`a` INT NOT NULL", schema.metadata("a").columnString());
    assertEquals("`b` VARCHAR(10)", schema.metadata("b").columnString());
  }

  @Test
  public void testSkip() throws Exception {
    String schemaString = "id\n/*comment*/int\r,//comment\r\nname\nvarchar\t\t\t";
    TupleMetadata schema = SchemaExprParser.parseSchema(schemaString);
    assertEquals(2, schema.size());
    assertEquals("`id` INT", schema.metadata("id").columnString());
    assertEquals("`name` VARCHAR", schema.metadata("name").columnString());
  }

  @Test
  public void testCaseInsensitivity() throws Exception {
    String schema = "`Id` InTeGeR NoT NuLl";
    assertEquals("`Id` INT NOT NULL", SchemaExprParser.parseSchema(schema).metadata(0).columnString());
  }

  @Test
  public void testParseColumn() throws Exception {
    ColumnMetadata column = SchemaExprParser.parseColumn("col int not null");
    assertEquals("`col` INT NOT NULL", column.columnString());
  }

  @Test
  public void testNumericTypes() throws Exception {
    TupleMetadata schema = new SchemaBuilder()
      .addNullable("int_col", TypeProtos.MinorType.INT)
      .add("integer_col", TypeProtos.MinorType.INT)
      .addNullable("bigint_col", TypeProtos.MinorType.BIGINT)
      .add("float_col", TypeProtos.MinorType.FLOAT4)
      .addNullable("double_col", TypeProtos.MinorType.FLOAT8)
      .buildSchema();

    checkSchema("int_col int, integer_col integer not null, bigint_col bigint, " +
        "float_col float not null, double_col double", schema);
  }

  @Test
  public void testDecimalTypes() {
    TupleMetadata schema = new SchemaBuilder()
      .addNullable("col", TypeProtos.MinorType.VARDECIMAL)
      .add("col_p", TypeProtos.MinorType.VARDECIMAL, 5)
      .addDecimal("col_ps", TypeProtos.MinorType.VARDECIMAL, TypeProtos.DataMode.OPTIONAL, 10, 2)
      .buildSchema();

    List<String> schemas = Arrays.asList(
      "col dec, col_p dec(5) not null, col_ps dec(10, 2)",
      "col decimal, col_p decimal(5) not null, col_ps decimal(10, 2)",
      "col numeric, col_p numeric(5) not null, col_ps numeric(10, 2)"
    );

    schemas.forEach(
      s -> {
        try {
          checkSchema(s, schema);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    );
  }

  @Test
  public void testBooleanType() throws Exception {
    TupleMetadata schema = new SchemaBuilder()
      .addNullable("col", TypeProtos.MinorType.BIT)
      .buildSchema();

    checkSchema("col boolean", schema);
  }

  @Test
  public void testCharacterTypes() {
    String schemaPattern = "col %1$s, col_p %1$s(50) not null";

    Map<String, TypeProtos.MinorType> properties = new HashMap<>();
    properties.put("char", TypeProtos.MinorType.VARCHAR);
    properties.put("character", TypeProtos.MinorType.VARCHAR);
    properties.put("character varying", TypeProtos.MinorType.VARCHAR);
    properties.put("varchar", TypeProtos.MinorType.VARCHAR);
    properties.put("binary", TypeProtos.MinorType.VARBINARY);
    properties.put("varbinary", TypeProtos.MinorType.VARBINARY);

    properties.forEach((key, value) -> {

      TupleMetadata schema = new SchemaBuilder()
        .addNullable("col", value)
        .add("col_p", value, 50)
        .buildSchema();

      try {
        checkSchema(String.format(schemaPattern, key), schema);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testTimeTypes() throws Exception {
    TupleMetadata schema = new SchemaBuilder()
      .addNullable("time_col", TypeProtos.MinorType.TIME)
      .addNullable("time_prec_col", TypeProtos.MinorType.TIME, 3)
      .add("date_col", TypeProtos.MinorType.DATE)
      .addNullable("timestamp_col", TypeProtos.MinorType.TIMESTAMP)
      .addNullable("timestamp_prec_col", TypeProtos.MinorType.TIMESTAMP, 3)
      .buildSchema();

    checkSchema("time_col time, time_prec_col time(3), date_col date not null, " +
        "timestamp_col timestamp, timestamp_prec_col timestamp(3)", schema);
  }

  @Test
  public void testInterval() throws Exception {
    TupleMetadata schema = new SchemaBuilder()
      .addNullable("interval_year_col", TypeProtos.MinorType.INTERVALYEAR)
      .addNullable("interval_month_col", TypeProtos.MinorType.INTERVALYEAR)
      .addNullable("interval_day_col", TypeProtos.MinorType.INTERVALDAY)
      .addNullable("interval_hour_col", TypeProtos.MinorType.INTERVALDAY)
      .addNullable("interval_minute_col", TypeProtos.MinorType.INTERVALDAY)
      .addNullable("interval_second_col", TypeProtos.MinorType.INTERVALDAY)
      .addNullable("interval_col", TypeProtos.MinorType.INTERVAL)
      .buildSchema();

    checkSchema("interval_year_col interval year, interval_month_col interval month, " +
        "interval_day_col interval day, interval_hour_col interval hour, interval_minute_col interval minute, " +
        "interval_second_col interval second, interval_col interval", schema);
  }

  @Test
  public void testArray() throws Exception {
    TupleMetadata schema = new SchemaBuilder()
      .addArray("simple_array", TypeProtos.MinorType.INT)
      .addRepeatedList("nested_array")
        .addArray(TypeProtos.MinorType.INT)
      .resumeSchema()
      .addMapArray("struct_array")
        .addNullable("m1", TypeProtos.MinorType.INT)
        .addNullable("m2", TypeProtos.MinorType.VARCHAR)
      .resumeSchema()
      .addRepeatedList("nested_array_struct")
        .addMapArray()
          .addNullable("nm1", TypeProtos.MinorType.INT)
          .addNullable("nm2", TypeProtos.MinorType.VARCHAR)
        .resumeList()
      .resumeSchema()
      .buildSchema();

    checkSchema("simple_array array<int>"
        + ", nested_array array<array<int>>"
        + ", struct_array array<struct<m1 int, m2 varchar>>"
        + ", nested_array_struct array<array<struct<nm1 int, nm2 varchar>>>",
      schema);

  }

  @Test
  public void testStruct() throws Exception {
    TupleMetadata schema = new SchemaBuilder()
      .addMap("struct_col")
        .addNullable("int_col", TypeProtos.MinorType.INT)
        .addArray("array_col", TypeProtos.MinorType.INT)
        .addMap("nested_struct")
          .addNullable("m1", TypeProtos.MinorType.INT)
          .addNullable("m2", TypeProtos.MinorType.VARCHAR)
        .resumeMap()
      .resumeSchema()
      .buildSchema();

    checkSchema("struct_col struct<int_col int, array_col array<int>, nested_struct struct<m1 int, m2 varchar>>", schema);
  }

  @Test
  public void testModeForSimpleType() throws Exception {
    TupleMetadata schema = SchemaExprParser.parseSchema("id int not null, name varchar");
    assertFalse(schema.metadata("id").isNullable());
    assertTrue(schema.metadata("name").isNullable());
  }

  @Test
  public void testModeForStructType() throws Exception {
    TupleMetadata schema  = SchemaExprParser.parseSchema("m struct<m1 int not null, m2 varchar>");
    ColumnMetadata map = schema.metadata("m");
    assertTrue(map.isMap());
    assertEquals(TypeProtos.DataMode.REQUIRED, map.mode());

    TupleMetadata mapSchema = map.mapSchema();
    assertFalse(mapSchema.metadata("m1").isNullable());
    assertTrue(mapSchema.metadata("m2").isNullable());
  }

  @Test
  public void testModeForRepeatedType() throws Exception {
    TupleMetadata schema = SchemaExprParser.parseSchema(
      "a array<int>, aa array<array<int>>, ma array<struct<m1 int not null, m2 varchar>>");

    assertTrue(schema.metadata("a").isArray());

    ColumnMetadata nestedArray = schema.metadata("aa");
    assertTrue(nestedArray.isArray());
    assertTrue(nestedArray.childSchema().isArray());

    ColumnMetadata mapArray = schema.metadata("ma");
    assertTrue(mapArray.isArray());
    assertTrue(mapArray.isMap());
    TupleMetadata mapSchema = mapArray.mapSchema();
    assertFalse(mapSchema.metadata("m1").isNullable());
    assertTrue(mapSchema.metadata("m2").isNullable());
  }

  @Test
  public void testFormat() throws Exception {
    String value = "`a` DATE NOT NULL FORMAT 'yyyy-MM-dd'";
    TupleMetadata schema = SchemaExprParser.parseSchema(value);
    ColumnMetadata columnMetadata = schema.metadata("a");
    assertEquals("yyyy-MM-dd", columnMetadata.format());
    assertEquals(value, columnMetadata.columnString());
  }

  @Test
  public void testDefault() throws Exception {
    String value = "`a` INT NOT NULL DEFAULT '12'";
    TupleMetadata schema = SchemaExprParser.parseSchema(value);
    ColumnMetadata columnMetadata = schema.metadata("a");
    assertTrue(columnMetadata.decodeDefaultValue() instanceof Integer);
    assertEquals(12, columnMetadata.decodeDefaultValue());
    assertEquals("12", columnMetadata.defaultValue());
    assertEquals(value, columnMetadata.columnString());
  }

  @Test
  public void testFormatAndDefault() throws Exception {
    String value = "`a` DATE NOT NULL FORMAT 'yyyy-MM-dd' DEFAULT '2018-12-31'";
    TupleMetadata schema = SchemaExprParser.parseSchema(value);
    ColumnMetadata columnMetadata = schema.metadata("a");
    assertTrue(columnMetadata.decodeDefaultValue() instanceof LocalDate);
    assertEquals(new LocalDate(2018, 12, 31), columnMetadata.decodeDefaultValue());
    assertEquals("2018-12-31", columnMetadata.defaultValue());
    assertEquals(value, columnMetadata.columnString());
  }

  @Test
  public void testColumnProperties() throws Exception {
    String value = "`a` INT NOT NULL PROPERTIES { 'k1' = 'v1', 'k2' = 'v2' }";
    TupleMetadata schema = SchemaExprParser.parseSchema(value);

    ColumnMetadata columnMetadata = schema.metadata("a");

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("k1", "v1");
    properties.put("k2", "v2");

    assertEquals(properties, columnMetadata.properties());
    assertEquals(value, columnMetadata.columnString());
  }

  @Test
  public void testEmptySchema() throws Exception {
    String value = "()";
    TupleMetadata schema = SchemaExprParser.parseSchema(value);
    assertEquals(0, schema.size());
  }

  @Test
  public void testEmptySchemaWithProperties() throws Exception {
    String value = "() properties { `drill.strict` = `false` }";
    TupleMetadata schema = SchemaExprParser.parseSchema(value);
    assertTrue(schema.isEmpty());
    assertEquals("false", schema.property("drill.strict"));
  }

  @Test
  public void testSchemaWithProperties() throws Exception {
    String value = "(col int properties { `drill.blank-as` = `0` } ) " +
      "properties { `drill.strict` = `false`, `drill.my-prop` = `abc` }";
    TupleMetadata schema = SchemaExprParser.parseSchema(value);
    assertEquals(1, schema.size());

    ColumnMetadata column = schema.metadata("col");
    assertEquals("0", column.property("drill.blank-as"));

    assertEquals("false", schema.property("drill.strict"));
    assertEquals("abc", schema.property("drill.my-prop"));
  }

  private void checkSchema(String schemaString, TupleMetadata expectedSchema) throws IOException {
    TupleMetadata actualSchema = SchemaExprParser.parseSchema(schemaString);

    assertEquals(expectedSchema.size(), actualSchema.size());
    assertEquals(expectedSchema.properties(), actualSchema.properties());

    expectedSchema.toMetadataList().forEach(
      expectedMetadata -> {
        ColumnMetadata actualMetadata = actualSchema.metadata(expectedMetadata.name());
        assertEquals(expectedMetadata.columnString(), actualMetadata.columnString());
      }
    );
  }

}
