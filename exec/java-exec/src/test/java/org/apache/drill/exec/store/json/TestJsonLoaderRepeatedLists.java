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
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.objArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.singleObjArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.fail;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * JSON supports repeated list. But, tests with the rest of the code
 * suggest that this type is not supported by some other operators.
 * Hence, the functionality is best tested in isolation with just
 * the JSON reader and scan framework, without the rest of Drill.
 */

@Category(RowSetTests.class)
public class TestJsonLoaderRepeatedLists extends BaseTestJsonLoader {

  @Test
  public void testBoolean2D() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [[true, false], [false, true]]}\n" +
        "{a: [[true], [false]]}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIT, 2)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(objArray(intArray(1, 0), intArray(0, 1)))
        .addSingleCol(objArray(intArray(1), intArray(0)))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testInt2D() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [[1, 10], [2, 20]]}\n" +
        "{a: [[1], [-1]]}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT, 2)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(objArray(longArray(1L, 10L), longArray(2L, 20L)))
        .addSingleCol(objArray(longArray(1L), longArray(-1L)))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testFloat2D() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [[1.25, 10.5], [2.25, 20.5]]}\n" +
        "{a: [[1], [-1]]}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.FLOAT8, 2)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(objArray(doubleArray(1.25D, 10.5D), doubleArray(2.25D, 20.5D)))
        .addSingleCol(objArray(doubleArray(1D), doubleArray(-1D)))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testString2D() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [[\"first\", \"second\"], [\"third\", \"fourth\"]]}\n" +
        "{a: [[\"fifth\"], [\"sixth\"]]}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.VARCHAR, 2)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(objArray(strArray("first", "second"), strArray("third", "fourth")))
        .addSingleCol(objArray(strArray("fifth"), strArray("sixth")))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  @Test
  public void testObject2D() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [[{b: 1, c: \"first\"},\n" +
        "      {b: 2, c: \"second\"}],\n" +
        "     [{b: 3, c: \"third\"},\n" +
        "      {b: 4, c: \"fourth\"}]]}\n" +
        "{a: [[{b: 5, c: \"fifth\"}],\n" +
        "     [{b: 6, c: \"sixth\"}]]}";
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addRepeatedList("a")
          .addMapArray()
            .addNullable("b", MinorType.BIGINT)
            .addNullable("c", MinorType.VARCHAR)
            .resumeList()
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(
            objArray(
                objArray(mapValue(1L, "first"), mapValue(2L, "second")),
                objArray(mapValue(3L, "third"), mapValue(4L, "fourth"))))
        .addSingleCol(
            objArray(
                singleObjArray(mapValue(5L, "fifth")),
                singleObjArray(mapValue(6L, "sixth"))))
        .build();

    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Leading empties forces the parser to defer determining
   * the actual type until a non-null, non-empty token appears.
   */

  @Test
  public void testLeadingEmpties() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [[]]}\n" +
        "{a: [[], null]}\n" +
        "{a: [[1, 10], [2, 20]]}\n";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT, 2)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(singleObjArray(longArray()))
        .addSingleCol(objArray(longArray(), longArray()))
        .addSingleCol(objArray(longArray(1L, 10L), longArray(2L, 20L)))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Trailing empties are easier, we know the type, just fill
   * in an empty array.
   */

  @Test
  public void testTrailingEmpties() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [[1, 10], [2, 20]]}\n" +
        "{a: [[]]}\n" +
        "{a: [[], null]}\n" +
        "{a: [[1], [-1]]}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT, 2)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(objArray(longArray(1L, 10L), longArray(2L, 20L)))
        .addSingleCol(singleObjArray(longArray()))
        .addSingleCol(objArray(longArray(), longArray()))
        .addSingleCol(objArray(longArray(1L), longArray(-1L)))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Trailing nulls forces the parser to defer knowledge of the
   * type again and again, gradually learning more about the type
   * on each new row.
   */

  @Test
  public void testLeadingAmbiguity() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: null}\n" +
        "{a: []}\n" +
        "{a: [[]]}\n" +
        "{a: [[1, 10], [2, 20]]}\n";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT, 2)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(objArray())
        .addSingleCol(objArray())
        .addSingleCol(singleObjArray(longArray()))
        .addSingleCol(objArray(longArray(1L, 10L), longArray(2L, 20L)))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * Drill can't handle a null in an array before Drill knows
   * that the array is multi-dimensional. Drill could, in theory,
   * handle this case by introducing yet another intermediate state
   * that "remembers" that the array can contain nulls. That is left
   * as an exercise for later.
   */

  @Test
  public void testleadingNulls() {
    expectError("{a: [null]} {a: [[10]]}");
  }

  /**
   * The same set of records work, however, if presented in
   * an order that shows Drill the array is 2D before it
   * presents a null for the inner array. This kind of
   * ambiguity will likely drive production users nuts...
   */

  @Test
  public void testTrailingNulls() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [[10]]} {a: [null]}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT, 2)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(singleObjArray(longArray(10L)))
        .addSingleCol(singleObjArray(longArray())) // null same as []
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * The repeated list allows higher-dimensional arrays
   * (AKA tensors). Encoding these in JSON and Java is a bit
   * tedious, however...
   */

  @Test
  public void testInt3D() {
    final JsonTester tester = jsonTester();
    final String json =
        "{a: [[[1, 2], [3, 4]],\n" +
        "     [[5, 6], [7, 8]]]}";
    final RowSet results = tester.parse(json);
    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.BIGINT, 3)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(
            objArray(
                objArray(
                    longArray(1L, 2L), longArray(3L, 4L)),
                objArray(
                    longArray(5L, 6L), longArray(7L, 8L))))
        .build();
    RowSetUtilities.verify(expected, results);
    tester.close();
  }

  /**
   * The empty array support tested above works as long as the
   * "answer" to the array type can be found within the batch.
   * However, there is an ambiguity if the first batch never reveals
   * the type. All we can do is guess. We guess "text mode" so that
   * the answer is compatible with a scalar value in the next batch.
   */

  @Test
  public void testUnresolvedAmbiguity() {
    final String json =
        "{a: null}\n" +
        "{a: []}\n" +
        "{a: [[]]}\n" +
        "{a: [[1, 10], [2, 20]]}\n";
    final MultiBatchJson tester = new MultiBatchJson(json);

    // Read first three records into a batch. Since we've not yet seen
    // a type, the 2D array type will be resolved as a text field.

    RowSet results = tester.parse(3);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.VARCHAR, 2)
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(objArray())
        .addSingleCol(objArray())
        .addSingleCol(singleObjArray(strArray()))
        .build();
    RowSetUtilities.verify(expected, results);

    // Second batch, read remaining records as text mode.

    results = tester.parse();
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(objArray(strArray("1", "10"), strArray("2", "20")))
        .build();
    RowSetUtilities.verify(expected, results);

    tester.close();
  }

  /**
   * But, if the next batch contains another array, all heck breaks
   * loose...
   */

  @Test
  public void testAmbiguityWrongGuess() {
    final String json =
        "{a: null}\n" +
        "{a: []}\n" +
        "{a: [[]]}\n" +
        "{a: [[[]]]}\n";
    final MultiBatchJson tester = new MultiBatchJson(json);

    // Read first three records into a batch. Since we've not yet seen
    // a type, the 2D array type will be resolved as a text field.

    final RowSet results = tester.parse(3);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addArray("a", MinorType.VARCHAR, 2)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(objArray())
        .addSingleCol(objArray())
        .addSingleCol(singleObjArray(strArray()))
        .build();
    RowSetUtilities.verify(expected, results);

    // Second batch, fail due to conflict of array and text mode.

    try {
      tester.parse();
      fail();
    } catch (final UserException e) {
      // Expected;
    }

    tester.close();
  }
}
