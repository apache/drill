package org.apache.drill.exec.store.phoenix;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSet.SingleRowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.physical.rowSet.RowSetWriter;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;

public class TestRowSet extends SubOperatorTest {

  @Test
  public void testRowSet() {
    final TupleMetadata schema = new SchemaBuilder()
        .add("id", MinorType.INT)
        .add("name",MinorType.VARCHAR)
        .buildSchema();

    final RowSet rowSet = new RowSetBuilder(fixture.allocator(), schema)
        .addRow(1, "luocong")
        .addRow(2, "sunny")
        .build();

    rowSet.print();
    rowSet.clear();

    DirectRowSet directRowSet = DirectRowSet.fromSchema(fixture.allocator(), schema);
    RowSetWriter writer = directRowSet.writer();
    writer.scalar("id").setInt(1);
    writer.scalar("name").setString("luocong");
    writer.scalar("id").setInt(2);
    writer.scalar("name").setString("sunny");
    writer.save();
    SingleRowSet record = writer.done();
    record.print();
    record.clear();
  }
}
