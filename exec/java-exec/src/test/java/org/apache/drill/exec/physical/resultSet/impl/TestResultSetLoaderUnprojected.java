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
package org.apache.drill.exec.physical.resultSet.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.physical.resultSet.project.Projections;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSet.SingleRowSet;
import org.apache.drill.exec.physical.rowSet.RowSetTestUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;

/**
 * Verify the correct functioning of the "dummy" columns created
 * for unprojected columns.
 */
public class TestResultSetLoaderUnprojected  extends SubOperatorTest {

  @Test
  public void testScalar()
  {
    List<SchemaPath> selection = RowSetTestUtils.projectList("a");
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .add("b", MinorType.INT)
        .buildSchema();
    ResultSetOptions options = new ResultSetOptionBuilder()
        .projection(Projections.parse(selection))
        .readerSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);

    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata actualSchema = rootWriter.tupleSchema();
    assertEquals(2, actualSchema.size());
    assertEquals("a", actualSchema.column(0).getName());
    assertEquals("b", actualSchema.column(1).getName());
    assertTrue(rootWriter.column("a").isProjected());
    assertFalse(rootWriter.column("b").isProjected());
    rsLoader.startBatch();
    for (int i = 1; i < 3; i++) {
      rootWriter.start();
      rootWriter.scalar(0).setInt(i);
      rootWriter.scalar(1).setInt(i * 5);
      rootWriter.save();
    }
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1)
        .addRow(2)
        .build();
    RowSet actual = fixture.wrap(rsLoader.harvest());
    RowSetUtilities.verify(expected, actual);
    rsLoader.close();
  }

  @Test
  public void testScalarArray()
  {
    List<SchemaPath> selection = RowSetTestUtils.projectList("a");
    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addArray("b", MinorType.INT)
        .buildSchema();
    ResultSetOptions options = new ResultSetOptionBuilder()
        .projection(Projections.parse(selection))
        .readerSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);

    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata actualSchema = rootWriter.tupleSchema();
    assertEquals(2, actualSchema.size());
    assertEquals("a", actualSchema.column(0).getName());
    assertEquals("b", actualSchema.column(1).getName());
    assertTrue(rootWriter.column("a").isProjected());
    assertFalse(rootWriter.column("b").isProjected());
    rsLoader.startBatch();
    for (int i = 1; i < 3; i++) {
      rootWriter.start();
      rootWriter.scalar(0).setInt(i);
      for (int j = 0; j < 3; j++) {
        ArrayWriter aw = rootWriter.array(1);
        ScalarWriter sw = rootWriter.array(1).scalar();
        sw.setInt(i * 5 + j);
        aw.save();
      }
      rootWriter.save();
    }
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1)
        .addRow(2)
        .build();
    RowSet actual = fixture.wrap(rsLoader.harvest());
    RowSetUtilities.verify(expected, actual);
    rsLoader.close();
  }

  @Test
  public void testMap()
  {
    List<SchemaPath> selection = RowSetTestUtils.projectList("a");
    TupleMetadata schema = new SchemaBuilder()
        .addMap("a")
          .add("foo", MinorType.INT)
          .resumeSchema()
        .addMap("b")
          .add("foo", MinorType.INT)
          .resumeSchema()
        .buildSchema();
    ResultSetOptions options = new ResultSetOptionBuilder()
        .projection(Projections.parse(selection))
        .readerSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);

    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata actualSchema = rootWriter.tupleSchema();
    assertEquals(2, actualSchema.size());
    assertEquals("a", actualSchema.column(0).getName());
    assertEquals("b", actualSchema.column(1).getName());
    assertTrue(rootWriter.column("a").isProjected());
    assertFalse(rootWriter.column("b").isProjected());
    rsLoader.startBatch();
    for (int i = 1; i < 3; i++) {
      rootWriter.start();
      rootWriter.tuple(0).scalar("foo").setInt(i);
      rootWriter.tuple(1).scalar("foo").setInt(i * 5);
      rootWriter.save();
    }
    TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("a")
          .add("foo", MinorType.INT)
          .resumeSchema()
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(RowSetUtilities.mapValue(1))
        .addSingleCol(RowSetUtilities.mapValue(2))
        .build();
    RowSet actual = fixture.wrap(rsLoader.harvest());
    RowSetUtilities.verify(expected, actual);
    rsLoader.close();
  }

  @Test
  public void testMapElements()
  {
    List<SchemaPath> selection = RowSetTestUtils.projectList("a.foo");
    TupleMetadata schema = new SchemaBuilder()
        .addMap("a")
          .add("foo", MinorType.INT)
          .add("bar", MinorType.INT)
          .resumeSchema()
        .buildSchema();
    ResultSetOptions options = new ResultSetOptionBuilder()
        .projection(Projections.parse(selection))
        .readerSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);

    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata actualSchema = rootWriter.tupleSchema();
    assertEquals(1, actualSchema.size());
    assertEquals("a", actualSchema.metadata(0).name());
    assertEquals(2, actualSchema.metadata(0).tupleSchema().size());
    assertEquals("foo", actualSchema.metadata(0).tupleSchema().metadata(0).name());
    assertEquals("bar", actualSchema.metadata(0).tupleSchema().metadata(1).name());
    assertTrue(rootWriter.column("a").isProjected());
    assertTrue(rootWriter.tuple("a").column(0).isProjected());
    assertFalse(rootWriter.tuple("a").column(1).isProjected());
    rsLoader.startBatch();
    for (int i = 1; i < 3; i++) {
      rootWriter.start();
      TupleWriter aWriter = rootWriter.tuple(0);
      aWriter.scalar("foo").setInt(i);
      aWriter.scalar("bar").setInt(i * 5);
      rootWriter.save();
    }
    TupleMetadata expectedSchema = new SchemaBuilder()
        .addMap("a")
          .add("foo", MinorType.INT)
          .resumeSchema()
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(RowSetUtilities.mapValue(1))
        .addSingleCol(RowSetUtilities.mapValue(2))
        .build();
    RowSet actual = fixture.wrap(rsLoader.harvest());
    RowSetUtilities.verify(expected, actual);
    rsLoader.close();
  }

  @Test
  public void testMapArray()
  {
    List<SchemaPath> selection = RowSetTestUtils.projectList("a");
    TupleMetadata schema = new SchemaBuilder()
        .addMapArray("a")
          .add("foo", MinorType.INT)
          .resumeSchema()
        .addMapArray("b")
          .add("foo", MinorType.INT)
          .resumeSchema()
        .buildSchema();
    ResultSetOptions options = new ResultSetOptionBuilder()
        .projection(Projections.parse(selection))
        .readerSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);

    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata actualSchema = rootWriter.tupleSchema();
    assertEquals(2, actualSchema.size());
    assertEquals("a", actualSchema.column(0).getName());
    assertEquals("b", actualSchema.column(1).getName());
    assertTrue(rootWriter.column("a").isProjected());
    assertFalse(rootWriter.column("b").isProjected());
    rsLoader.startBatch();
    for (int i = 0; i < 2; i++) {
      rootWriter.start();
      ArrayWriter aWriter = rootWriter.array(0);
      ArrayWriter bWriter = rootWriter.array(1);
      for (int j = 0; j < 2; j++) {
        aWriter.tuple().scalar(0).setInt(i * 2 + j + 1);
        bWriter.tuple().scalar(0).setInt((i * 2 + j) * 5);
        aWriter.save();
        bWriter.save();
      }
      rootWriter.save();
    }
    TupleMetadata expectedSchema = new SchemaBuilder()
        .addMapArray("a")
          .add("foo", MinorType.INT)
          .resumeSchema()
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(
            RowSetUtilities.mapArray(
                RowSetUtilities.mapValue(1),
                RowSetUtilities.mapValue(2)))
        .addSingleCol(
            RowSetUtilities.mapArray(
                RowSetUtilities.mapValue(3),
                RowSetUtilities.mapValue(4)))
        .build();
    RowSet actual = fixture.wrap(rsLoader.harvest());
    RowSetUtilities.verify(expected, actual);
    rsLoader.close();
  }

  @Test
  public void testVariant()
  {
    List<SchemaPath> selection = RowSetTestUtils.projectList("a");
    TupleMetadata schema = new SchemaBuilder()
        .addUnion("a")
          .resumeSchema()
        .addUnion("b")
          .resumeSchema()
        .buildSchema();
    ResultSetOptions options = new ResultSetOptionBuilder()
        .projection(Projections.parse(selection))
        .readerSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);

    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata actualSchema = rootWriter.tupleSchema();
    assertEquals(2, actualSchema.size());
    assertEquals("a", actualSchema.column(0).getName());
    assertEquals("b", actualSchema.column(1).getName());
    assertTrue(rootWriter.column("a").isProjected());
    assertFalse(rootWriter.column("b").isProjected());
    rsLoader.startBatch();
    rootWriter.start();
    rootWriter.variant(0).scalar(MinorType.INT).setInt(1);
    rootWriter.variant(1).scalar(MinorType.INT).setInt(5);
    rootWriter.save();
    rootWriter.start();
    rootWriter.variant(0).scalar(MinorType.VARCHAR).setString("2");
    rootWriter.variant(1).scalar(MinorType.VARCHAR).setString("10");
    rootWriter.save();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addUnion("a")
          .addType(MinorType.INT)
          .addType(MinorType.VARCHAR)
          .resumeSchema()
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addRow(1)
        .addRow("2")
        .build();
    RowSet actual = fixture.wrap(rsLoader.harvest());
    RowSetUtilities.verify(expected, actual);
    rsLoader.close();
  }

  @Test
  public void testList()
  {
    List<SchemaPath> selection = RowSetTestUtils.projectList("a");
    TupleMetadata schema = new SchemaBuilder()
        .addList("a")
          .addType(MinorType.INT)
          .resumeSchema()
        .addList("b")
          .addType(MinorType.INT)
          .resumeSchema()
        .buildSchema();
    ResultSetOptions options = new ResultSetOptionBuilder()
        .projection(Projections.parse(selection))
        .readerSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);

    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata actualSchema = rootWriter.tupleSchema();
    assertEquals(2, actualSchema.size());
    assertEquals("a", actualSchema.column(0).getName());
    assertEquals("b", actualSchema.column(1).getName());
    assertTrue(rootWriter.column("a").isProjected());
    assertFalse(rootWriter.column("b").isProjected());
    rsLoader.startBatch();
    ArrayWriter aw = rootWriter.array(0);
    ScalarWriter swa = aw.scalar();
    ArrayWriter bw = rootWriter.array(1);
    ScalarWriter swb = bw.scalar();
    for (int i = 1; i < 3; i++) {
      rootWriter.start();
      for (int j = 0; j < 3; j++) {
        swa.setInt(i * 10 + j);
        swb.setInt(i * 100 + j);
        aw.save();
        bw.save();
      }
      rootWriter.save();
    }
    TupleMetadata expectedSchema = new SchemaBuilder()
        .addList("a")
          .addType(MinorType.INT)
          .resumeSchema()
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(RowSetUtilities.listValue(10, 11, 12))
        .addSingleCol(RowSetUtilities.listValue(20, 21, 22))
        .build();
    RowSet actual = fixture.wrap(rsLoader.harvest());
    RowSetUtilities.verify(expected, actual);
    rsLoader.close();
  }

  @Test
  public void test2DList()
  {
    List<SchemaPath> selection = RowSetTestUtils.projectList("a");
    TupleMetadata schema = new SchemaBuilder()
        .addRepeatedList("a")
          .addArray(MinorType.INT)
          .resumeSchema()
        .addRepeatedList("b")
          .addArray(MinorType.INT)
          .resumeSchema()
        .buildSchema();
    ResultSetOptions options = new ResultSetOptionBuilder()
        .projection(Projections.parse(selection))
        .readerSchema(schema)
        .build();
    ResultSetLoader rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);

    RowSetLoader rootWriter = rsLoader.writer();
    TupleMetadata actualSchema = rootWriter.tupleSchema();
    assertEquals(2, actualSchema.size());
    assertEquals("a", actualSchema.column(0).getName());
    assertEquals("b", actualSchema.column(1).getName());
    assertTrue(rootWriter.column("a").isProjected());
    assertFalse(rootWriter.column("b").isProjected());
    rsLoader.startBatch();
    ArrayWriter aw = rootWriter.array(0);
    ArrayWriter aw2 = aw.array();
    ScalarWriter swa = aw2.scalar();
    ArrayWriter bw = rootWriter.array(1);
    ArrayWriter bw2 = bw.array();
    ScalarWriter swb = bw2.scalar();
    for (int i = 1; i < 3; i++) {
      rootWriter.start();
      for (int j = 0; j < 3; j++) {
        for (int k = 0; k < 3; k++) {
          swa.setInt(i * 10 + j * 3 + k);
          swb.setInt(i * 100 + j * 30 + k);
          aw2.save();
          bw2.save();
        }
        aw.save();
        bw.save();
      }
      rootWriter.save();
    }

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addRepeatedList("a")
          .addArray(MinorType.INT)
          .resumeSchema()
        .buildSchema();
    SingleRowSet expected = fixture.rowSetBuilder(expectedSchema)
        .addSingleCol(RowSetUtilities.listValue(
            RowSetUtilities.listValue(10, 11, 12),
            RowSetUtilities.listValue(13, 14, 15),
            RowSetUtilities.listValue(16, 17, 18)))
        .addSingleCol(RowSetUtilities.listValue(
            RowSetUtilities.listValue(20, 21, 22),
            RowSetUtilities.listValue(23, 24, 25),
            RowSetUtilities.listValue(26, 27, 28)))
        .build();
    RowSet actual = fixture.wrap(rsLoader.harvest());
    RowSetUtilities.verify(expected, actual);
    rsLoader.close();
  }
}
