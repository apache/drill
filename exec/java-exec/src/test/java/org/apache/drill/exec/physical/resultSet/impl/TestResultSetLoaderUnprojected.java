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
import org.apache.drill.exec.physical.rowSet.RowSetTestUtils;
import org.apache.drill.exec.physical.rowSet.RowSet.SingleRowSet;
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
    rootWriter.scalar(0).setInt(1);
    rootWriter.scalar(1).setInt(5);
    rootWriter.save();
    rootWriter.start();
    rootWriter.scalar(0).setString("2");
    rootWriter.scalar(1).setString("10");
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

  public void testList()
  {

  }

  public void test2DList()
  {

  }

  public void testDict()
  {

  }

}
