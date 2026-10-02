package org.apache.drill.exec.store.pcapng;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.physical.impl.validate.BatchValidator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetOptionBuilder;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.junit.Test;

public class TmpLoader {
  @Test
  public void probe() {
    BufferAllocator allocator = RootAllocatorFactory.newRoot(DrillConfig.create());
    TupleMetadata dns = new SchemaBuilder().addNullable("id", MinorType.INT)
        .addMapArray("questions").addNullable("name", MinorType.VARCHAR).resumeSchema().buildSchema();
    TupleMetadata data = new SchemaBuilder().add(MetadataUtils.newMap("dns", dns)).buildSchema();
    TupleMetadata schema = new SchemaBuilder().addNullable("p", MinorType.VARCHAR)
        .add(MetadataUtils.newMap("parsed_data", data)).buildSchema();
    ResultSetLoader rsl = new ResultSetLoaderImpl(allocator, new ResultSetOptionBuilder().readerSchema(schema).build());
    RowSetLoader loader = rsl.writer();
    TupleWriter parsed = loader.tuple("parsed_data");
    rsl.startBatch();
    for (int row = 0; row < 4; row++) {
      loader.start();
      if (row < 2) {
        TupleWriter d = parsed.tuple("dns");
        d.scalar("id").setInt(row);
        ArrayWriter q = d.array("questions");
        q.tuple().scalar("name").setString("q" + row);
        q.save();
      }
      loader.save();
    }
    VectorContainer c = rsl.harvest();
    System.out.println("PROBE rows=" + c.getRecordCount() + " valid=" + BatchValidator.validate(c));
    c.zeroVectors();
    rsl.close();
    allocator.close();
  }
}
