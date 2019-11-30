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

import static org.apache.drill.test.rowSet.RowSetUtilities.mapArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.project.projSet.ProjectionSetFactory;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.physical.resultSet.impl.RowSetTestUtils;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.JsonLoader;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonOptions;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the JSON loader itself: the various syntax combinations that the loader understands
 * or rejects. Assumes that the JSON parser itself works, and that the underlying result set
 * loader properly handles column types, projections and so on.
 */

@Category(RowSetTests.class)
public class TestJsonLoaderBasics extends BaseTestJsonLoader {

  @Test
  public void testEmpty() {
    final JsonTester tester = jsonTester();
    final RowSet results = tester.parse("");
    assertEquals(0, results.rowCount());
    results.clear();
    tester.close();
  }

  @Test
  public void testEmptyTuple() {
    final JsonTester tester = jsonTester();
    final String json = "{} {} {}";
    final RowSet results = tester.parse(json);
    assertEquals(3, results.rowCount());
    results.clear();
    tester.close();
  }

  @Test
  public void testBoolean() {
    final JsonTester tester = jsonTester();
    final String json = "{a: true} {a: false} {a: null}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1)
        .addRow(0)
        .addSingleCol(null)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testInteger() {
    final JsonTester tester = jsonTester();
    final String json = "{a: 0} {a: 100} {a: null}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(0L)
        .addRow(100L)
        .addSingleCol(null)
        .build();

    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testFloat() {
    final JsonTester tester = jsonTester();

    // Note: integers allowed after first float.

    final String json = "{a: 0.0} {a: 100.5} {a: 5} {a: null}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(0D)
        .addRow(100.5D)
        .addRow(5D)
        .addSingleCol(null)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testExtendedFloat() {
    final JsonOptions options = new JsonOptions();
    options.allowNanInf = true;
    final JsonTester tester = jsonTester(options);
    final String json =
        "{a: 0.0} {a: 100.5} {a: -200.5} {a: NaN}\n" +
        "{a: Infinity} {a: -Infinity} {a: null}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(0D)
        .addRow(100.5D)
        .addRow(-200.5D)
        .addRow(Double.NaN)
        .addRow(Double.POSITIVE_INFINITY)
        .addRow(Double.NEGATIVE_INFINITY)
        .addSingleCol(null)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testIntegerAsFloat() {
    final JsonOptions options = new JsonOptions();
    options.readNumbersAsDouble = true;
    final JsonTester tester = jsonTester(options);
    final String json = "{a: 0} {a: 100} {a: null} {a: 123.45}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.FLOAT8)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(0D)
        .addRow(100D)
        .addSingleCol(null)
        .addRow(123.45D)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testString() {
    final JsonTester tester = jsonTester();
    final String json = "{a: \"\"} {a: \"hi\"} {a: null}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow("")
        .addRow("hi")
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testRootTuple() {
    final JsonTester tester = jsonTester();
    final String json =
      "{id: 1, name: \"Fred\", balance: 100.0}\n" +
      "{id: 2, name: \"Barney\"}\n" +
      "{id: 3, name: \"Wilma\", balance: 500.00}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("id", MinorType.BIGINT)
        .addNullable("name", MinorType.VARCHAR)
        .addNullable("balance", MinorType.FLOAT8)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1L, "Fred", 100.00D)
        .addRow(2L, "Barney", null)
        .addRow(3L, "Wilma", 500.0D)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testMissingEndObject() {
    expectError("{a: 0} {a: 100");
  }

  @Test
  public void testMissingValue() {
    expectError("{a: 0} {a: ");
  }

  @Test
  public void testMissingEndOuterArray() {
    expectError("[{a: 0}, {a: 100}");
  }

  @Test
  public void testNullInArray() {
    expectError("{a: [10, 20, null]}");
  }

  @Test
  public void testEmptyKey() {
    expectError("{\"\": 10}");
  }

  @Test
  public void testBlankKey() {
    expectError("{\"  \": 10}");
  }

  @Test
  public void testLeadingTrailingWhitespace() {
    final JsonTester tester = jsonTester();
    final String json = "{\" a\": 10, \" b\": 20, \" c \": 30}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addNullable("b", MinorType.BIGINT)
        .addNullable("c", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(10L, 20L, 30L)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Verify that names are case insensitive, first name determine's
   * Drill's column name.
   */

  @Test
  public void testCaseInsensitive() {
    final JsonTester tester = jsonTester();
    final String json = "{a: 10} {A: 20} {\" a \": 30}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(10L)
        .addRow(20L)
        .addRow(30L)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Verify that the first name wins when determining case.
   */

  @Test
  public void testCaseInsensitive2() {
    final JsonTester tester = jsonTester();
    final String json = "{Bob: 10} {bOb: 20} {BoB: 30}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("Bob", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(10L)
        .addRow(20L)
        .addRow(30L)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Verify that, when names are duplicated, the last value wins.
   */

  @Test
  public void testDuplicateNames() {
    final JsonTester tester = jsonTester();
    final String json = "{a: 10, A: 20, \" a \": 30}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(30L)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testNestedTuple() {
    final JsonTester tester = jsonTester();
    final String json =
        "{id: 1, customer: { name: \"fred\" }}\n" +
        "{id: 2, customer: { name: \"barney\" }}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("id", MinorType.BIGINT)
        .addMap("customer")
          .addNullable("name", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1L, mapValue("fred"))
        .addRow(2L, mapValue("barney"))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testNullTuple() {
    final JsonTester tester = jsonTester();
    final String json =
        "{id: 1, customer: {name: \"fred\"}}\n" +
        "{id: 2, customer: {name: \"barney\"}}\n" +
        "{id: 3, customer: null}\n" +
        "{id: 4}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("id", MinorType.BIGINT)
        .addMap("customer")
          .addNullable("name", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(1L, mapValue("fred"))
        .addRow(2L, mapValue("barney"))
        .addRow(3L, mapValue((String) null))
        .addRow(4L, mapValue((String) null))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testRootArray() {
    final JsonTester tester = jsonTester();
    final String json = "[{a: 0}, {a: 100}, {a: null}]";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(0L)
        .addRow(100L)
        .addSingleCol(null)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testRootArrayDisallowed() {
    final JsonOptions options = new JsonOptions();
    options.skipOuterList = false;
    final JsonTester tester = jsonTester(options);
    final String json = "[{a: 0}, {a: 100}, {a: null}]";
    expectError(tester, json);
  }

  /**
   * Verify that non-projected maps are just "free-wheeled", the JSON loader
   * just seeks the matching end brace, ignoring content semantics.
   */

  @Test
  public void testNonProjected() {
    final String json =
        "{a: 10, b: {c: 100, c: 200, d: {x: null, y: 30}}}\n" +
        "{a: 20, b: { }}\n" +
        "{a: 30, b: null}\n" +
        "{a: 40, b: {c: \"foo\", d: [{}, {x: {}}]}}\n" +
        "{a: 50, b: [{c: 100, c: 200, d: {x: null, y: 30}}, 10]}\n" +
        "{a: 60, b: []}\n" +
        "{a: 70, b: null}\n" +
        "{a: 80, b: [\"foo\", [{}, {x: {}}]]}\n" +
        "{a: 90, b: 55.5} {a: 100, b: 10} {a: 110, b: \"foo\"}";
    final ResultSetOptions rsOptions = new OptionBuilder()
        .setProjection(
            ProjectionSetFactory.build(RowSetTestUtils.projectList("a")))
        .build();
    final ResultSetLoader tableLoader = new ResultSetLoaderImpl(fixture.allocator(), rsOptions);
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
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    assertTrue(expectedSchema.isEquivalent(result.schema()));
    final RowSetReader reader = result.reader();
    for (int i = 10; i <= 110; i += 10) {
      assertTrue(reader.next());
      assertEquals(i, reader.scalar(0).getLong());
    }
    assertFalse(reader.next());
    result.clear();

    try {
      inStream.close();
    } catch (final IOException e) {
      fail();
    }
    loader.close();
    tableLoader.close();
  }

  /**
   * Test the JSON parser's limited recovery abilities.
   *
   * @see <a href="https://issues.apache.org/jira/browse/DRILL-4653">DRILL-4653</a>
   */

  @Test
  public void testErrorRecovery() {
    final JsonOptions options = new JsonOptions();
    options.skipMalformedRecords = true;
    final JsonTester tester = jsonTester(options);
    final String json = "{\"a: 10}\n{a: 20}\n{a: 30}";
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
//        .addRow(20L) // Recovery will eat the second record.
        .addRow(30L)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Test handling of unrecoverable parse errors. This test must change if
   * we resolve DRILL-5953 and allow better recovery.
   *
   * @see <a href="https://issues.apache.org/jira/browse/DRILL-5953">DRILL-5953</a>
   */

  @Test
  public void testUnrecoverableError() {
    final JsonOptions options = new JsonOptions();
    options.skipMalformedRecords = true;
    final JsonTester tester = jsonTester(options);
    expectError(tester, "{a: }\n{a: 20}\n{a: 30}");
  }

  /**
   * Test based on TestJsonReader.testAllTextMode
   * Verifies the complex case of an array of maps that contains an array of
   * strings (from all text mode). Also verifies projection.
   */

  @Test
  public void testMapSelect() {
    final JsonOptions options = new JsonOptions();
    options.allTextMode = true;
    final JsonTester tester = jsonTester(options);
    tester.loaderOptions.setProjection(
        ProjectionSetFactory.build(RowSetTestUtils.projectList("field_5")));
    final RowSet results = tester.parseFile("store/json/schema_change_int_to_string.json");
//    results.print();

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addMapArray("field_5")
          .addArray("inner_list", MinorType.VARCHAR)
          .addArray("inner_list_2", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(mapArray())
        .addSingleCol(mapArray(
            mapValue(strArray("1", "", "6"), strArray()),
            mapValue(strArray("3", "8"), strArray()),
            mapValue(strArray("12", "", "4", "null", "5"), strArray())))
        .addSingleCol(mapArray(
            mapValue(strArray("5", "", "6.0", "1234"), strArray()),
            mapValue(strArray("7", "8.0", "12341324"), strArray("1", "2", "2323.443e10", "hello there")),
            mapValue(strArray("3", "4", "5"), strArray("10", "11", "12"))))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  // TODO: Union support
}
