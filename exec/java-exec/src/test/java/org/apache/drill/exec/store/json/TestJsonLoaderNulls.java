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
import static org.apache.drill.test.rowSet.RowSetUtilities.longArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.singleMap;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.project.projSet.ProjectionSetFactory;
import org.apache.drill.exec.physical.resultSet.impl.RowSetTestUtils;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonOptions;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.TypeNegotiator;
import org.apache.drill.shaded.guava.com.google.common.base.Joiner;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Drill requires types to be known on the first row. JSON files can be quite lazy about
 * revealing the type: there may be many missing values, null values, or empty arrays
 * before the parser finally sees a token that suggests the column type. The JSON loader
 * has "null deferral" logic to postpone picking a column type until a type token finally
 * appears (or until the end of the batch, when the pick is forced.)
 * <p>
 * Empty arrays for multi-dimensional lists are tested in
 * {@link TestJsonLoaderRepeatedLists}.
 */

@Category(RowSetTests.class)
public class TestJsonLoaderNulls extends BaseTestJsonLoader {

  @Test
  public void testAllNulls() {
    final JsonTester tester = jsonTester();
    final String json = "{a: null} {a: null} {a: null}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(null)
        .addSingleCol(null)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testAllNullsInNested() {
    final JsonTester tester = jsonTester();
    final String json = "{a: {b: null}} {a: {b: null}} {a: {b: null}}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("a")
          .addNullable("b", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(singleMap(null))
        .addSingleCol(singleMap(null))
        .addSingleCol(singleMap(null))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testDeferredScalarNull() {
    final JsonTester tester = jsonTester();
    final String json = "{a: null} {a: null} {a: 10}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(null)
        .addRow(10L)
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testDeferredScalarNullInNested() {
    final JsonTester tester = jsonTester();
    final String json = "{a: {b: null}} {a: {b: null}} {a: {b: 10}}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("a")
          .addNullable("b", MinorType.BIGINT)
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(singleMap(null))
        .addSingleCol(singleMap(null))
        .addSingleCol(singleMap(10L))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testDeferredScalarNullAsText() {
    final String json = "{a: null} {a: null} {a: null} {a: 10} {a: \"foo\"}";
    final MultiBatchJson tester = new MultiBatchJson(json);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    RowSet results = tester.parse(2);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(null)
        .build();
    RowSetUtilities.verify(expected, results);

    // Second batch, read remaining records as text mode.

    results = tester.parse();
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol("10")
        .addSingleCol("foo")
        .build();
    RowSetUtilities.verify(expected, results);

    tester.close();
  }

  @Test
  public void testDeferredScalarNullAsType() {
    final String json = "{a: null} {a: null} {a: null} {a: 10} {a: 20}";

    final JsonOptions options = new JsonOptions();
    options.context = "test Json";
    options.typeNegotiator = new TypeNegotiator() {

      // Only one field.

      @Override
      public MajorType typeOf(List<String> path) {
        assertEquals(1, path.size());
        assertEquals("a", path.get(0));
        return Types.optional(MinorType.BIGINT);
      }
    };
    final MultiBatchJson tester = new MultiBatchJson(options, json);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    RowSet results = tester.parse(2);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(null)
        .build();
    RowSetUtilities.verify(expected, results);

    // Second batch, read remaining records as given type.

    results = tester.parse();
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(10L)
        .addSingleCol(20L)
        .build();
    RowSetUtilities.verify(expected, results);

    tester.close();
  }

  @Test
  public void testNestedDeferredScalarNullAsType() {
    final String json =
        "{a: {b: null, c: null, d: null}, e: null}\n" +
        "{a: {b: null, c: null, d: null}, e: null}\n" +
        "{a: {b: null, c: null, d: null}, e: null}\n" +
        "{a: {b: 10, c: \"fred\", d: [1.5, 2.5]}, e: 10.25}\n";

    final JsonOptions options = new JsonOptions();
    options.context = "test Json";
    options.typeNegotiator = new TypeNegotiator() {

      // Find types for three fields.

      @Override
      public MajorType typeOf(List<String> path) {
        final String name = Joiner.on(".").join(path);
        switch (name) {
        case "a.b": return Types.optional(MinorType.BIGINT);
        case "a.c": return Types.optional(MinorType.VARCHAR);
        case "a.d": return Types.repeated(MinorType.FLOAT8);
        case "e": return null;
        default:
          fail("Unexpected path: " + name);
          return null;
        }
      }
    };
    final MultiBatchJson tester = new MultiBatchJson(options, json);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    RowSet results = tester.parse(2);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("a")
          .addNullable("b", MinorType.BIGINT)
          .addNullable("c", MinorType.VARCHAR)
          .addArray("d", MinorType.FLOAT8)
          .resumeSchema()
        .addNullable("e", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(mapValue(null, null, doubleArray()), null)
        .addRow(mapValue(null, null, doubleArray()), null)
        .build();
    RowSetUtilities.verify(expected, results);

    // Second batch, read remaining records as given type.

    results = tester.parse();
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(mapValue(null, null, doubleArray()), null)
        .addRow(mapValue(10L, "fred", doubleArray(1.5, 2.5)), "10.25")
        .build();
    RowSetUtilities.verify(expected, results);

    tester.close();
  }

  @Test
  public void testDeferredScalarArray() {
    final JsonTester tester = jsonTester();
    final String json = "{a: []} {a: null} {a: [10, 20]}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(longArray())
        .addSingleCol(longArray())
        .addSingleCol(longArray(10L, 20L))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testDeferredArrayAsText() {
    final String json = "{a: []} {a: null} {a: []} {a: [10, 20]} {a: [\"foo\", \"bar\"]}";
    final MultiBatchJson tester = new MultiBatchJson(json);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    RowSet results = tester.parse(2);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(strArray())
        .addSingleCol(strArray())
        .build();
    RowSetUtilities.verify(expected, results);

    // Second batch, read remaining records as text mode.

    results = tester.parse();
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(strArray())
        .addSingleCol(strArray("10", "20"))
        .addSingleCol(strArray("foo", "bar"))
        .build();
    RowSetUtilities.verify(expected, results);

    tester.close();
  }

  @Test
  public void testDeferredArrayAsType() {
    final String json =
        "{a: [], b: []}\n" +
        "{a: null, b: null}\n" +
        "{a: [], b: []}\n" +
        "{a: [10, 20], b: [10.5, \"fred\"]}\n" +
        "{a: [30, 40], b: [null, false]}";

    final JsonOptions options = new JsonOptions();
    options.context = "test Json";
    options.typeNegotiator = new TypeNegotiator() {
      @Override
      public MajorType typeOf(List<String> path) {
        assertEquals(1, path.size());
        switch (path.get(0)) {

        // Note: type for b is optional, not compatible with
        // an array, so is ignored but the type will be used.

        case "a": return Types.optional(MinorType.BIGINT);

        // No hint for b, text mode will be used.

        case "b": return null;
        default:
          fail();
          return null;
        }
      }
    };
    final MultiBatchJson tester = new MultiBatchJson(options, json);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    RowSet results = tester.parse(2);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT)
        .addArray("b", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(longArray(), strArray())
        .addRow(longArray(), strArray())
        .build();
    RowSetUtilities.verify(expected, results);

    // Second batch, read remaining records as long.

    results = tester.parse();
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addRow(longArray(), strArray())
        .addRow(longArray(10L, 20L), strArray("10.5", "fred"))
        .addRow(longArray(30L, 40L), strArray("", "false"))
        .build();
    RowSetUtilities.verify(expected, results);

    tester.close();
  }

  /**
   * Double deferral. First null causes a deferred null. Then,
   * the empty array causes a deferred array. Finally, the third
   * array announces the type.
   */

  @Test
  public void testDeferredNullToArray() {
    final JsonTester tester = jsonTester();
    final String json = "{a: null} {a: []} {a: [10, 20]}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(longArray())
        .addSingleCol(longArray())
        .addSingleCol(longArray(10L, 20L))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Test that the JSON reader uses a projection hint to
   * determine that a null type is an array.
   */

  @Test
  public void testArrayProjectionHint() {

    // Read the one and only record into a batch. When we saw the
    // null value for b, we should have used the knowledge that b must
    // be an array (based on the projection of b[0]), to make it an array.
    // Then, at the end of the batch, we guess that the array is of
    // type VARCHAR.

    final String json =
        "{a: 1, b: null}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    tester.loaderOptions.setProjection(
        ProjectionSetFactory.build(RowSetTestUtils.projectList("a", "b[0]")));
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addArray("b", MinorType.VARCHAR)
        .buildSchema();

    final RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1L, strArray())
        .build();
    RowSetUtilities.verify(expected, results);
  }

  /**
   * Test that the JSON reader uses a projection hint to
   * determine that a null type is a map.
   */

  @Test
  public void testObjectProjectionHint() {

    // Read the one and only record into a batch. When we saw the
    // null value for b, we should have used the knowledge that b must
    // be a map (based on the projection of a.b), to make it a map
    // (which contains no columns.)

    final String json =
        "{a: 1, b: null}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    tester.loaderOptions.setProjection(
        ProjectionSetFactory.build(RowSetTestUtils.projectList("a", "b.c")));
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addMap("b")
          .resumeSchema()
        .buildSchema();

    final RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1L, mapValue())
        .build();
    RowSetUtilities.verify(expected, results);
  }

}
