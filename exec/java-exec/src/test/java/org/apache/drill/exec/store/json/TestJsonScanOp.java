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

import static org.apache.drill.test.rowSet.RowSetUtilities.longArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapArray;
import static org.apache.drill.test.rowSet.RowSetUtilities.mapValue;
import static org.apache.drill.test.rowSet.RowSetUtilities.singleMap;
import static org.apache.drill.test.rowSet.RowSetUtilities.strArray;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.impl.scan.BaseScanOperatorExecTest.BaseScanFixtureBuilder;
import org.apache.drill.exec.physical.impl.scan.ScanOperatorExec;
import org.apache.drill.exec.physical.impl.scan.ScanTestUtils.ScanFixture;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.JsonLoader;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonOptions;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(RowSetTests.class)
public class TestJsonScanOp extends SubOperatorTest {

  private static class TestJsonReader implements ManagedReader<SchemaNegotiator> {

    private ResultSetLoader tableLoader;
    private final String filePath;
    private InputStream stream;
    private JsonLoader jsonLoader;
    private final JsonOptions options;

    public TestJsonReader(String filePath, JsonOptions options) {
      this.filePath = filePath;
      this.options = options;
    }

    @Override
    public boolean open(SchemaNegotiator negotiator) {
      stream = new BufferedInputStream(getClass().getResourceAsStream(filePath));
      tableLoader = negotiator.build();
      jsonLoader = new JsonLoaderImpl(stream, tableLoader.writer(), options);
      return true;
    }

    @Override
    public boolean next() {
      boolean more = true;
      while (tableLoader.writer().start()) {
        if (! jsonLoader.next()) {
          more = false;
          break;
        }
        tableLoader.writer().save();
      }
      jsonLoader.endBatch();
      return more;
    }

    @Override
    public void close() {
      try {
        stream.close();
      } catch (IOException e) {
        // Ignore;
      }
    }
  }

  /**
   * Test the case where the reader does not play the "first batch contains
   * only schema" game, and instead returns data. The Scan operator will
   * split the first batch into two: one with schema only, another with
   * data.
   */

  @Test
  public void testScanOperator() {

    BaseScanFixtureBuilder builder = new BaseScanFixtureBuilder();
    JsonOptions options = new JsonOptions();
    options.allTextMode = true;
    builder.addReader(new TestJsonReader("/store/json/schema_change_int_to_string.json", options));
    builder.setProjection("field_3", "field_5");
    ScanFixture scanFixture = builder.build();
    ScanOperatorExec scanOp = scanFixture.scanOp;

    assertTrue(scanOp.buildSchema());
    RowSet result = fixture.wrap(scanOp.batchAccessor().container());
    result.clear();
    assertTrue(scanOp.next());
    result = fixture.wrap(scanOp.batchAccessor().container());

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("field_3")
          .addNullable("inner_1", MinorType.VARCHAR)
          .addNullable("inner_2", MinorType.VARCHAR)
          .addMapArray("inner_3")
            .addNullable("inner_object_field_1", MinorType.VARCHAR)
            .resumeMap()
          .resumeSchema()
        .addMapArray("field_5")
          .addArray("inner_list", MinorType.VARCHAR)
          .addArray("inner_list_2", MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();

    RowSetUtilities.strArray();
    RowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(mapValue(null, null, mapArray()),
                mapArray())
        .addRow(mapValue("2", null, mapArray()),
                mapArray(
                  mapValue(strArray("1", "", "6"), strArray()),
                  mapValue(strArray("3", "8"), strArray()),
                  mapValue(strArray("12", "", "4", "null", "5"), strArray())))
        .addRow(mapValue("5", "3", mapArray(singleMap(null), singleMap("10"))),
            mapArray(
                mapValue(strArray("5", "", "6.0", "1234"), strArray()),
                mapValue(strArray("7", "8.0", "12341324"),
                         strArray("1", "2", "2323.443e10", "hello there")),
                mapValue(strArray("3", "4", "5"), strArray("10", "11", "12"))))
        .build();

    RowSetUtilities.verify(expected, result);
    scanFixture.close();
  }

  @Test
  public void testScanProjectMapSubset() {

    BaseScanFixtureBuilder builder = new BaseScanFixtureBuilder();
    JsonOptions options = new JsonOptions();
    builder.addReader(new TestJsonReader("/store/json/schema_change_int_to_string.json", options));
    builder.setProjection("field_3.inner_1", "field_3.inner_2");
    ScanFixture scanFixture = builder.build();
    ScanOperatorExec scanOp = scanFixture.scanOp;

    assertTrue(scanOp.buildSchema());
    RowSet result = fixture.wrap(scanOp.batchAccessor().container());
    assertTrue(scanOp.next());
    result = fixture.wrap(scanOp.batchAccessor().container());

    TupleMetadata schema = new SchemaBuilder()
        .addMap("field_3")
          .addNullable("inner_1", MinorType.BIGINT)
          .addNullable("inner_2", MinorType.BIGINT)
          .resumeSchema()
        .build();

    RowSet expected = fixture.rowSetBuilder(schema)
        .addSingleCol(mapValue(null, null))
        .addSingleCol(mapValue(2L, null))
        .addSingleCol(mapValue(5L, 3L))
        .build();
    RowSetUtilities.verify(expected, result);
    scanFixture.close();
  }

  @Test
  public void testScanProjectMapArraySubsetAndNull() {

    BaseScanFixtureBuilder builder = new BaseScanFixtureBuilder();
    JsonOptions options = new JsonOptions();
    options.allTextMode = true;
    builder.addReader(new TestJsonReader("/store/json/schema_change_int_to_string.json", options));
    builder.setProjection("field_5.inner_list", "field_5.dummy");
    builder.builder().setNullType(Types.optional(MinorType.VARCHAR));
    ScanFixture scanFixture = builder.build();
    ScanOperatorExec scanOp = scanFixture.scanOp;

    assertTrue(scanOp.buildSchema());
    RowSet result = fixture.wrap(scanOp.batchAccessor().container());
    assertTrue(scanOp.next());
    result = fixture.wrap(scanOp.batchAccessor().container());

    TupleMetadata schema = new SchemaBuilder()
        .addMapArray("field_5")
          .addArray("inner_list", MinorType.VARCHAR)
          .addNullable("dummy", MinorType.VARCHAR)
          .resumeSchema()
        .build();

    RowSet expected = fixture.rowSetBuilder(schema)
        .addSingleCol(mapArray())
        .addSingleCol(mapArray(
            mapValue(strArray("1", "", "6"), null),
            mapValue(strArray("3", "8"), null),
            mapValue(strArray("12", "", "4", "null", "5"), null)))
        .addSingleCol(mapArray(
            mapValue(strArray("5", "", "6.0", "1234"), null),
            mapValue(strArray("7", "8.0", "12341324"), null),
            mapValue(strArray("3", "4", "5"), null)))
        .build();
    RowSetUtilities.verify(expected, result);
    scanFixture.close();
  }

  @Test
  public void testScanProject() {

    BaseScanFixtureBuilder builder = new BaseScanFixtureBuilder();
    JsonOptions options = new JsonOptions();
    builder.addReader(new TestJsonReader("/store/json/schema_change_int_to_string.json", options));

    // Projection omits field_2 which has an ambiguous type. Since
    // the field is not materialized, the ambiguity is benign.
    // (If this test triggers an error, perhaps a change has caused
    // the column to become materialized.)

    builder.setProjection("field_1", "field_3.inner_1", "field_3.inner_2", "field_4.inner_1",
        "non_existent_at_root", "non_existent.nested.field");
    builder.builder().setNullType(Types.optional(MinorType.VARCHAR));
    ScanFixture scanFixture = builder.build();
    ScanOperatorExec scanOp = scanFixture.scanOp;

    assertTrue(scanOp.buildSchema());
    RowSet result = fixture.wrap(scanOp.batchAccessor().container());
    assertTrue(scanOp.next());
    result = fixture.wrap(scanOp.batchAccessor().container());

    // Projects all columns (since the revised scan operator handles missing-column
    // projection.) Note that the result includes two batches, including the first empty
    // batch.

    TupleMetadata schema = new SchemaBuilder()
        .addArray("field_1", MinorType.BIGINT)
        .addMap("field_3")
          .addNullable("inner_1", MinorType.BIGINT)
          .addNullable("inner_2", MinorType.BIGINT)
          .resumeSchema()
        .addMap("field_4")
          .addArray("inner_1", MinorType.BIGINT)
          .resumeSchema()
        .addNullable("non_existent_at_root", MinorType.VARCHAR)
        .addMap("non_existent")
          .addMap("nested")
            .addNullable("field", MinorType.VARCHAR)
            .resumeMap()
          .resumeSchema()
        .build();

    Object nullMap = singleMap(singleMap(null));
    RowSet expected = fixture.rowSetBuilder(schema)
        .addRow(longArray(1L), mapValue(null, null), singleMap(longArray()), null, nullMap )
        .addRow(longArray(5L), mapValue(2L, null), singleMap(longArray(1L, 2L, 3L)), null, nullMap)
        .addRow(longArray(5L, 10L, 15L), mapValue(5L, 3L), singleMap(longArray(4L, 5L, 6L)), null, nullMap)
        .build();
    RowSetUtilities.verify(expected, result);
    scanFixture.close();
  }
}
