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
package org.apache.drill.exec.store.json;

import static org.apache.drill.test.rowSet.RowSetUtilities.doubleArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.intArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.longArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.singleMap;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.JsonLoader;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonOptions;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(RowSetTests.class)
public class TestJsonLoaderArray extends BaseTestJsonLoader {

  @Test
  public void testBooleanArray() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [true, false]}\n" +
        "{a: []} {a: null}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(intArray(1, 0))
        .addRow(intArray())
        .addRow(intArray())
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testIntegerArray() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [1, 100]}\n" +
        "{a: []} {a: null}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(longArray(1L, 100L))
        .addSingleCol(longArray())
        .addSingleCol(longArray())
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testFloatArray() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [1.0, 100]}\n" +
        "{a: []} {a: null}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.FLOAT8)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(doubleArray(1D, 100D))
        .addRow(doubleArray())
        .addRow(doubleArray())
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testIntegerArrayAsFloat() {
    final JsonOptions options = new JsonOptions();
    options.readNumbersAsDouble = true;
    final JsonTester tester = jsonTester(options);
    final String json =
        "{a: [1, 100]}\n" +
        "{a: []}\n" +
        "{a: null}\n" +
        "{a: [12.5, 123.45]}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.FLOAT8)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(doubleArray(1.0, 100.0))
        .addRow(doubleArray())
        .addRow(doubleArray())
        .addRow(doubleArray(12.5, 123.45))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testIntererFloatConflictInArray() {
    expectError("{a: [10, 12.5]");
  }

  @Test
  public void testStringArray() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [\"\", \"foo\"]}\n" +
        "{a: []} {a: null}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.VARCHAR)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(strArray("", "foo"))
        .addSingleCol(strArray())
        .addSingleCol(strArray())
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testAllTextArray() {
    final JsonOptions options = new JsonOptions();
    options.allTextMode = true;
    final JsonTester tester = jsonTester(options);
    final String json =
        "{a: [\"foo\", true, false, 10, null, 20.0] }";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.VARCHAR)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(strArray("foo", "true", "false", "10", "", "20.0"))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testTupleArray() {
    final JsonTester tester = jsonTester();
    final String json =
        "{id: 1, customer: {name: \"fred\"}, orders: [\n" +
        "  {id: 1001, status: \"closed\"},\n" +
        "  {id: 1002, status: \"open\"}]}\n" +
        "{id: 2, customer: {name: \"barney\"}, orders: []}\n" +
        "{id: 3, customer: {name: \"wilma\"}, orders: null}\n" +
        "{id: 4, customer: {name: \"betty\"}}\n" +
        "{id: 5, customer: {name: \"pebbles\"}, orders: [\n" +
        "  {id: 1003, status: \"canceled\"}]}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("id", MinorType.BIGINT)
        .addMap("customer")
          .addNullable("name", MinorType.VARCHAR)
          .resumeSchema()
        .addMapArray("orders")
          .addNullable("id", MinorType.BIGINT)
          .addNullable("status", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1L, mapValue("fred"), mapArray(
            mapValue(1001L, "closed"),
            mapValue(1002L, "open")))
        .addRow(2L, mapValue("barney"), mapArray())
        .addRow(3L, mapValue("wilma"), mapArray())
        .addRow(4L, mapValue("betty"), mapArray())
        .addRow(5L, mapValue("pebbles"), mapArray(
            mapValue(1003L, "canceled")))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testDeferredTupleArray() {
    final JsonTester tester = jsonTester();
    final String json = "{a: []} {a: null} {a: [{name: \"fred\"}, {name: \"barney\"}]}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addMapArray("a")
          .addNullable("name", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(mapArray())
        .addSingleCol(mapArray())
        .addSingleCol(mapArray(
            mapValue("fred"), mapValue("barney")))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testTupleArrayNullCols() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [{b: null}, {b:null}]} " +
        "{a: [{b: null}]} " +
        "{a: [{b: null}, {b:null}, {b:null}]}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addMapArray("a")
          .addNullable("b", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(mapArray(singleMap(null), singleMap(null)))
        .addSingleCol(mapArray(singleMap(null)))
        .addSingleCol(mapArray(singleMap(null), singleMap(null), singleMap(null)))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testTupleArrayNullColsNested() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [{b: {c: null}}, {b: {c: null}}]} " +
        "{a: [{b: {c: null}}]} " +
        "{a: [{b: {c: null}}, {b: {c: null}}, {b: {c: null}}]}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addMapArray("a")
          .addMap("b")
            .addNullable("c", MinorType.VARCHAR)
            .resumeMap()
          .resumeSchema()
        .buildSchema();
    Object[] mapValue = singleMap(singleMap(null));
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(mapArray(mapValue, mapValue))
        .addSingleCol(mapArray(mapValue))
        .addSingleCol(mapArray(mapValue, mapValue, mapValue))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testArrays() {
    final String json =
        "{a: null, b: []}\n" +
        "{a: [true, false], b: [10, 20], c: [10.5, 12.25], d: [\"foo\", \"bar\"]}";
    final ResultSetLoader tableLoader = new ResultSetLoaderImpl(fixture.allocator());
    final InputStream inStream = new
        ReaderInputStream(new StringReader(json));
    final JsonOptions options = new JsonOptions();
    options.context = "test Json";
    final JsonLoader loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);
    readBatch(tableLoader, loader);
    final RowSet result = fixture.wrap(tableLoader.harvest());

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIT)
        .addArray("b", MinorType.BIGINT)
        .addArray("c", MinorType.FLOAT8)
        .addArray("d", MinorType.VARCHAR)
        .buildSchema();

    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(intArray(), longArray(), doubleArray(), strArray())
        .addRow(intArray(1, 0), longArray(10L, 20L), doubleArray(10.5, 12.25), strArray("foo", "bar"))
        .build();

    RowSetUtilities.verify(expected, result);

    try {
      inStream.close();
    } catch (final IOException e) {
      fail();
    }
    loader.close();
    tableLoader.close();
  }

  /**
   * Drill supports 1-D arrays using repeated types. Drill does not
   * support 2-D or higher arrays. Instead, Drill reverts to "text
   * mode" for such arrays, capturing them as JSON text, allowing the
   * client to interpret them.
   */

  @Test
  @Ignore("All text mode does not handle structures")
  public void testArraysOld() {
    final String oneDArray = "[[1, 2], [3, 4]]";
    final String twoDArray = "[[[1, 2], [3, 4]], [[5, 6], [7, 8]]]";
    final String json =
        "{a: [10, 11]," +
        " b: " + oneDArray + "," +
        " c: " + twoDArray + "}\n" +

        // 2- and 3-D arrays are all text. So, allow changes
        // to cardinality.

        "{a: [20, 21]," +
        " b: " + twoDArray + "," +
        " c: " + oneDArray + "}";
    final ResultSetLoader tableLoader = new ResultSetLoaderImpl(fixture.allocator());
    final InputStream inStream = new
        ReaderInputStream(new StringReader(json));
    final JsonOptions options = new JsonOptions();
    options.context = "test Json";
    final JsonLoader loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    readBatch(tableLoader, loader);
    final RowSet result = fixture.wrap(tableLoader.harvest());

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT)
        .addNullable("b", MinorType.VARCHAR)
        .addNullable("c", MinorType.VARCHAR)
        .buildSchema();

    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(new long[] {10, 11}, oneDArray, twoDArray)
        .addRow(new long[] {20, 21}, twoDArray, oneDArray)
        .build();

    RowSetUtilities.verify(expected, result);

    try {
      inStream.close();
    } catch (final IOException e) {
      fail();
    }
    loader.close();
    tableLoader.close();
  }

  @Test
  public void testEmptyArray() {
    final String json =
        "{a: [], b: \"first\"} " +
        "{a: [], b: \"second\"} " +
        "{a: [], b: \"third\"}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    final RowSet results = tester.parse(json);

    // Order of columns reverses because array is not
    // materialized until end of batch.

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("b", MinorType.VARCHAR)
        .addArray("a", MinorType.VARCHAR)
        .buildSchema();

    final RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow("first", strArray())
        .addRow("second", strArray())
        .addRow("third", strArray())
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testEmptyNestedArray() {
    final String json =
        "{a: {b: []}, c: \"first\"} " +
        "{a: {b: []}, c: \"second\"} " +
        "{a: {b: []}, c: \"third\"}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("a")
          .addArray("b", MinorType.VARCHAR)
          .resumeSchema()
        .addNullable("c", MinorType.VARCHAR)
        .buildSchema();

    final RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(singleMap(strArray()), "first")
        .addRow(singleMap(strArray()), "second")
        .addRow(singleMap(strArray()), "third")
        .build();
    RowSetUtilities.verify(expected, results);
  }
}
