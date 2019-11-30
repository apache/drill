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

import static org.apache.drill.test.rowSet.RowSetUtilities.listValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.longArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.singleList;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
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
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests of the experimental JSON list support using the incomplete
 * list vector. The tests here show that the list vector works for
 * JSON. There are known problems, however, in other operators.
 */

@Category(RowSetTests.class)
public class TestJsonLoaderLists extends BaseTestJsonLoader {

  /**
   * Test scalar list support.
   */

  @Test
  public void testScalarList() {

    // Read the one and only record into a batch. When we saw the
    // null value for b, we should have used the knowledge that b must
    // be a map (based on the projection of a.b), to make it an map
    // (which contains no columns.)

    final String json =
        "{a: 1, b: [10, null, 20]}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    options.useListType = true;
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addList("b")
          .addType(MinorType.BIGINT)
          .resumeSchema()
        .buildSchema();

    final RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1L, longArray(10L, null, 20L))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testScalarListLeadingNull() {
    final String json =
        "{a: 1, b: [null, 10, 20]}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    options.useListType = true;
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addList("b")
          .addType(MinorType.BIGINT)
          .resumeSchema()
        .buildSchema();

    final RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1L, longArray(null, 10L, 20L))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testScalarListAllNull() {
    final String json =
        "{a: 1, b: [null, null, null]}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    options.useListType = true;
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("a", MinorType.BIGINT)
        .addList("b")
          .addType(MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();

    final RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1L, strArray(null, null, null))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testEmptyArray() {
    final String json =
        "{a: []} {a: []} {a: []}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    options.useListType = true;
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addList("a")
          .addType(MinorType.VARCHAR)
         .resumeSchema()
        .buildSchema();

    final RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(strArray())
        .addSingleCol(strArray())
        .addSingleCol(strArray())
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testDeferredListAsText() {
    final String json = "{a: []} {a: null} {a: []} {a: [10, 20]} {a: [\"foo\", \"bar\"]}";
    final ResultSetLoader tableLoader = new ResultSetLoaderImpl(fixture.allocator());
    final InputStream inStream = new
        ReaderInputStream(new StringReader(json));
    final JsonOptions options = new JsonOptions();
    options.useListType = true;
    options.context = "test Json";
    final JsonLoader loader = new JsonLoaderImpl(inStream, tableLoader.writer(), options);

    // Read first two records into a batch. Since we've not yet seen
    // a type, the null field will be realized as a text field.

    readBatch(tableLoader, loader, 2);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addList("a")
          .addType(MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(strArray())
        .addSingleCol(strArray())
        .build();
    RowSetUtilities.verify(expected,
        fixture.wrap(tableLoader.harvest()));

    // Second batch, read remaining records as text mode.

    readBatch(tableLoader, loader);
    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(strArray())
        .addSingleCol(strArray("10", "20"))
        .addSingleCol(strArray("foo", "bar"))
        .build();
    RowSetUtilities.verify(expected,
        fixture.wrap(tableLoader.harvest()));

    try {
      inStream.close();
    } catch (final IOException e) {
      fail();
    }
    loader.close();
    tableLoader.close();
  }

  @Test
  public void testListPromotionFromNull() {
    final String json = "{a: null} {a: []} {a: null} {a: [10, 20]}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    options.useListType = true;
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addList("a")
          .addType(MinorType.BIGINT)
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(longArray())
        .addSingleCol(null)
        .addSingleCol(longArray(10L, 20L))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testConflictingListTypes() {
    final JsonOptions options = new JsonOptions();
    options.skipOuterList = false;
    final JsonTester tester = jsonTester(options);
    final String json = "{a: [10]} {a: [\"oops\"]}";
    expectError(tester, json);
  }

  @Test
  public void testObjectList() {
    final String json =
        "{a: [{b: \"fred\", c: 10}, null, {b: \"barney\", c: 20}]}\n" +
        "{a: []} {a: null} {a: [{b: \"wilma\", c: 30}]}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    options.useListType = true;
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addList("a")
          .addMap()
            .addNullable("b", MinorType.VARCHAR)
            .addNullable("c", MinorType.BIGINT)
            .resumeUnion()
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(mapArray(mapValue("fred", 10L), null, mapValue("barney", 20L)))
        .addSingleCol(mapArray())
        .addSingleCol(null)
        .addSingleCol(mapArray(mapValue("wilma", 30L)))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testDeferredObjectList() {
    final String json =
        "{a: null} {a: []} {a: [null]}\n" +
        "{a: [{b: \"fred\", c: 10}, null, {b: \"barney\", c: 20}]}\n" +
        "{a: []} {a: null} {a: [{b: \"wilma\", c: 30}]}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    options.useListType = true;
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addList("a")
          .addMap()
            .addNullable("b", MinorType.VARCHAR)
            .addNullable("c", MinorType.BIGINT)
            .resumeUnion()
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(mapArray())
        .addSingleCol(mapArray((Object[]) null))
        .addSingleCol(mapArray(mapValue("fred", 10L), null, mapValue("barney", 20L)))
        .addSingleCol(mapArray())
        .addSingleCol(null)
        .addSingleCol(mapArray(mapValue("wilma", 30L)))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testListofListofScalar() {
    final String json =
        "{a: [[1, 2], [3, 4]]}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    options.useListType = true;
    final RowSet results = tester.parse(json);

    // Verify metadata

    final ListVector outer = (ListVector) results.container().getValueVector(0).getValueVector();
    final MajorType outerType = outer.getField().getType();
    assertEquals(1, outerType.getSubTypeCount());
    assertEquals(MinorType.LIST, outerType.getSubType(0));
    assertEquals(1, outer.getField().getChildren().size());

    final ListVector inner = (ListVector) outer.getDataVector();
    assertSame(inner.getField(), outer.getField().getChildren().iterator().next());
    final MajorType innerType = inner.getField().getType();
    assertEquals(1, innerType.getSubTypeCount());
    assertEquals(MinorType.BIGINT, innerType.getSubType(0));
    assertEquals(1, inner.getField().getChildren().size());

    final ValueVector data = inner.getDataVector();
    assertSame(data.getField(), inner.getField().getChildren().iterator().next());
    assertEquals(MinorType.BIGINT, data.getField().getType().getMinorType());
    assertEquals(DataMode.OPTIONAL, data.getField().getType().getMode());
    assertTrue(data instanceof NullableBigIntVector);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addList("a")
          .addList()
            .addType(MinorType.BIGINT)
            .resumeUnion()
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(listValue(listValue(1L, 2L), listValue(3L, 4L)))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testListofListofScalarWithNulls() {
    final String json =
        "{a: null}\n" +
        "{a: []}\n" +
        "{a: [null]}\n" +
        "{a: [[]]}\n" +
        "{a: [[null]]}\n" +
        "{a: [null, [\"a\", \"string\"]]}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    options.useListType = true;
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addList("a")
          .addList()
            .addType(MinorType.VARCHAR)
            .resumeUnion()
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(null)
        .addSingleCol(listValue())
        .addSingleCol(singleList(null))
        .addSingleCol(singleList(listValue()))
        .addSingleCol(singleList(singleList(null)))
        .addSingleCol(listValue(null, strArray("a", "string")))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testListofListofObject() {
    final String json =
        "{a: [[{a: 1, b: 2}, {a: 3, b: 4}],\n" +
             "[{a: 5, b: 6}, {a: 7, b: 8}]]}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    options.useListType = true;
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addList("a")
          .addList()
            .addMap()
              .addNullable("a", MinorType.BIGINT)
              .addNullable("b", MinorType.BIGINT)
              .resumeUnion()
            .resumeUnion()
          .resumeSchema()
        .buildSchema();
    final RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(listValue(listValue(mapValue(1L, 2L), mapValue(3L, 4L)),
                                listValue(mapValue(5L, 6L), mapValue(7L, 8L))))
        .build();
    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testListofListofObjectWithNulls() {
    final String json =
        "{a: [[{a: null, b: null}, {a: 1, b: 2}, null, {a: 3}],\n" +
             "null, [{b: 6}]]}\n" +
        "{a: null}\n" +
        "{a: []}\n" +
        "{a: [[], null]}";
    final JsonOptions options = new JsonOptions();
    final JsonTester tester = jsonTester(options);
    options.useListType = true;
    final RowSet results = tester.parse(json);

    final TupleMetadata expectedSchema = new SchemaBuilder()
        .addList("a")
          .addList()
            .addMap()
              .addNullable("a", MinorType.BIGINT)
              .addNullable("b", MinorType.BIGINT)
              .resumeUnion()
            .resumeUnion()
          .resumeSchema()
        .buildSchema();

    // Logically expected results.

    RowSet expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(listValue(listValue(mapValue(null, null),
                                          mapValue(1L, 2L),
                                          null,
                                          mapValue(3L, null)),
                                null,
                                singleList(mapValue(null, 6L))))
        .addSingleCol(null)
        .addSingleCol(listValue())
        .addSingleCol(listValue(listValue(), null))
        .build();
    new RowSetComparison(expected).verify(results);
    expected.clear();

    // Physical description of results

    expected = new RowSetBuilder(fixture.allocator(), expectedSchema)
        .addSingleCol(listValue(listValue(mapValue(null, null),
                                          mapValue(1L, 2L),
                                          mapValue(null, null), // Maps can't really be null
                                          mapValue(3L, null)),
                                null, // List entries can be null
                                singleList(mapValue(null, 6L))))
        .addSingleCol(null)
        .addSingleCol(listValue())
        .addSingleCol(listValue(listValue(), null))
        .build();
    RowSetUtilities.verify(expected, results);
  }
}
