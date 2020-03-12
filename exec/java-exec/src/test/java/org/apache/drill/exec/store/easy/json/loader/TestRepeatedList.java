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
package org.apache.drill.exec.store.easy.json.loader;

import static org.apache.drill.test.rowSet.RowSetUtilities.longArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.objArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.singleObjArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;

/**
 * Tests repeated lists to form a 2D array of various data types.
 */
public class TestRepeatedList extends BaseJsonLoaderTest {

  @Test
  public void test2DScalars() {
    String json =
        "{a: [[1, 2], [3, 4, 5]]}\n" +
        "{a: [[6], [7, 8]]}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addRepeatedList("a")
          .addArray(MinorType.BIGINT)
          .resumeSchema()
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(objArray(
            longArray(1L, 2L), longArray(3L, 4L, 5L)))
        .addSingleCol(objArray(
            longArray(6L), longArray(7L, 8L)))
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void test2DUnknown() {
    String json =
        "{a: []} {a: [[1, 2], [3, 4, 5]]}\n";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addRepeatedList("a")
          .addArray(MinorType.BIGINT)
          .resumeSchema()
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(null)
        .addSingleCol(objArray(
            longArray(1L, 2L), longArray(3L, 4L, 5L)))
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void test2DScalarWithSchema() {
    String json =
        "{a: null} {a: []} {a: [null]} {a: [[]]} {a: [[null]]}\n" +
        "{a: [[1, 2], [3, 4, 5]]}\n";
    TupleMetadata schema = new SchemaBuilder()
        .addRepeatedList("a")
          .addArray(MinorType.BIGINT)
          .resumeSchema()
        .build();

    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.providedSchema = schema;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    RowSet expected = fixture.rowSetBuilder(schema)
        .addSingleCol(objArray())
        .addSingleCol(objArray())
        .addSingleCol(singleObjArray(longArray()))
        .addSingleCol(singleObjArray(longArray()))
        .addSingleCol(singleObjArray(longArray(0L)))
        .addSingleCol(objArray(
            longArray(1L, 2L), longArray(3L, 4L, 5L)))
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void test2DUnknownForcedNull() {
    String json =
        "{a: []} {a: [[null]]}\n" +
        "{a: [[1, 2], [3, 4, 5]]}\n";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addRepeatedList("a")
          .addArray(MinorType.VARCHAR)
          .resumeSchema()
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(null)
        .addSingleCol(singleObjArray(strArray("")))
        .addSingleCol(objArray(
            strArray("1", "2"), strArray("3", "4", "5")))
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void test2DUnknownForcedEmptyArray() {
    String json =
        "{a: []} {a: [[]]} {a: [[1, 2], [3, 4, 5]]}\n";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addRepeatedList("a")
          .addArray(MinorType.VARCHAR)
          .resumeSchema()
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(null)
        .addSingleCol(singleObjArray(strArray()))
        .addSingleCol(objArray(
            strArray("1", "2"), strArray("3", "4", "5")))
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void test3DScalars() {
    String json =
        "{a: [[[1, 2]]]]}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    try {
      loader.next();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("arrays deeper than two levels"));
    }
    loader.close();
  }

  @Test
  public void test2DObjects() {
    String json =
        "{a: [[{b: 1}, {b: 2}], [{b: 3}, {b: 4}, {b: 5}]]}\n" +
        "{a: [[{b: 6}], [{b: 7}, {b: 8}]]}";
    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addRepeatedList("a")
          .addMapArray()
            .addNullable("b", MinorType.BIGINT)
            .resumeList()
          .resumeSchema()
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(objArray(
            objArray(mapValue(1L), mapValue(2L)),
            objArray(mapValue(3L), mapValue(4L), mapValue(5L))))
        .addSingleCol(objArray(
            singleObjArray(mapValue(6L)),
            objArray(mapValue(7L), mapValue(8L))))
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void test2DObjectsWithSchema() {
    String json =
        "{a: []} {a: [[null]]}\n" +
        "{a: [[{b: 1}, {b: 2}], [{b: 3}, {b: 4}, {b: 5}]]}\n" +
        "{a: [[{b: 6}], [{b: 7}, {b: 8}]]}";
    TupleMetadata schema = new SchemaBuilder()
        .addRepeatedList("a")
          .addMapArray()
            .addNullable("b", MinorType.BIGINT)
            .resumeList()
          .resumeSchema()
        .build();

    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.providedSchema = schema;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    RowSet expected = fixture.rowSetBuilder(schema)
        .addSingleCol(objArray())
        .addSingleCol(singleObjArray(mapValue((Long) null)))
        .addSingleCol(objArray(
            objArray(mapValue(1L), mapValue(2L)),
            objArray(mapValue(3L), mapValue(4L), mapValue(5L))))
        .addSingleCol(objArray(
            singleObjArray(mapValue(6L)),
            objArray(mapValue(7L), mapValue(8L))))
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }

  @Test
  public void test2DVariantWithSchema() {
    String json =
        "{a: []} {a: [[null]]}\n" +
        "{a: [[true, 10], [20.5, \"foo\"]]}" +
        "{a: [[{b: 1}, 2], [{b: 3}, \"four\", {b: 5}]]}\n";
    TupleMetadata schema = new SchemaBuilder()
        .addRepeatedList("a")
          .addList()
            .resumeList()
          .resumeSchema()
        .build();

    JsonLoaderFixture loader = new JsonLoaderFixture();
    loader.providedSchema = schema;
    loader.open(json);
    RowSet results = loader.next();
    assertNotNull(results);

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addRepeatedList("a")
          .addList()
            .addType(MinorType.BIGINT)
            .addType(MinorType.BIT)
            .addType(MinorType.VARCHAR)
            .addType(MinorType.FLOAT8)
            .addMap()
               .addNullable("b", MinorType.BIGINT)
              .resumeUnion()
            .resumeList()
          .resumeSchema()
        .build();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(objArray())
        .addSingleCol(singleObjArray(singleObjArray(null)))
        .addSingleCol(objArray(
            objArray(true, 10L),
            objArray(20.5D, "foo")))
        .addSingleCol(objArray(
            objArray(mapValue(1L), 2L),
            objArray(mapValue(3L), "four", mapValue(5L))))
        .build();
    RowSetUtilities.verify(expected, results);
    assertNull(loader.next());
    loader.close();
  }
}
