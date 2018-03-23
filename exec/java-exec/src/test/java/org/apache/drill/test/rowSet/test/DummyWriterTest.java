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
package org.apache.drill.test.rowSet.test;

import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter;
import org.apache.drill.exec.vector.accessor.writer.ColumnWriterFactory;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.Test;

public class DummyWriterTest extends SubOperatorTest {

  /**
   * Test only, bare-bones tuple writer used to gather the dummy
   * column writers.
   */

  public class RootWriterFixture extends AbstractTupleWriter {

    protected RootWriterFixture(TupleMetadata schema,
        List<AbstractObjectWriter> writers) {
      super(schema, writers);
    }
  }

  /**
   * Test dummy column writers for scalars and arrays of
   * scalars.
   */

  @Test
  public void testDummyScalar() {

    TupleMetadata schema = new SchemaBuilder()
        .add("a", MinorType.INT)
        .addArray("b", MinorType.VARCHAR)
        .buildSchema();
    List<AbstractObjectWriter> writers = new ArrayList<>();

    // We provide no vector. Factory should build us "dummy" writers.

    writers.add(ColumnWriterFactory.buildColumnWriter(schema.metadata("a"), null));
    writers.add(ColumnWriterFactory.buildColumnWriter(schema.metadata("b"), null));
    AbstractTupleWriter rootWriter = new RootWriterFixture(schema, writers);

    // Events are ignored.

    rootWriter.startWrite();
    rootWriter.startRow();

    // At present, dummy writers report no type (because they don't have one.)

    assertNull(rootWriter.scalar(0).valueType());

    // First column. Set int value.

    rootWriter.scalar(0).setInt(10);

    // Dummy writer does not do type checking. Write "wrong" type.
    // Should be allowed.

    rootWriter.scalar("a").setString("foo");

    // Column is required, but writer does no checking. Can set
    // a null value.

    rootWriter.column(0).scalar().setNull();

    // Second column: is an array.

    rootWriter.array(1).scalar().setString("bar");
    rootWriter.array(1).scalar().setString("mumble");

    // Again, type is not checked.

    rootWriter.array("b").scalar().setInt(200);

    // More ignored events.

    rootWriter.restartRow();
    rootWriter.saveRow();
    rootWriter.endWrite();
  }

  /**
   * Test a dummy map or map array. A (non-enforced) rule is that such maps
   * contain only dummy writers. The writers act like "real" writers.
   */

  @Test
  public void testDummyMap() {

    TupleMetadata schema = new SchemaBuilder()
        .addMap("m1")
          .add("a", MinorType.INT)
          .addArray("b", MinorType.VARCHAR)
          .resumeSchema()
        .addMapArray("m2")
          .add("c", MinorType.INT)
          .resumeSchema()
        .buildSchema();
    List<AbstractObjectWriter> writers = new ArrayList<>();

    {
      schema.metadata("m1").setProjected(false);
      TupleMetadata mapSchema = schema.metadata("m1").mapSchema();
      List<AbstractObjectWriter> members = new ArrayList<>();
      members.add(ColumnWriterFactory.buildColumnWriter(mapSchema.metadata("a"), null));
      members.add(ColumnWriterFactory.buildColumnWriter(mapSchema.metadata("b"), null));
      writers.add(ColumnWriterFactory.buildMapWriter(schema.metadata("m1"), null, members));
    }

    {
      schema.metadata("m2").setProjected(false);
      TupleMetadata mapSchema = schema.metadata("m2").mapSchema();
      List<AbstractObjectWriter> members = new ArrayList<>();
      members.add(ColumnWriterFactory.buildColumnWriter(mapSchema.metadata("c"), null));
      writers.add(ColumnWriterFactory.buildMapWriter(schema.metadata("m2"), null, members));
    }

    AbstractTupleWriter rootWriter = new RootWriterFixture(schema, writers);

    // Events are ignored.

    rootWriter.startWrite();
    rootWriter.startRow();

    // Dummy columns seem real.

    rootWriter.tuple("m1").scalar("a").setInt(20);
    rootWriter.tuple(0).array("b").scalar().setString("foo");

    // Dummy array map seems real.

    rootWriter.array("m2").tuple().scalar("c").setInt(30);
    rootWriter.array("m2").save();
    rootWriter.array(1).tuple().scalar(0).setInt(40);
    rootWriter.array(1).save();

    // More ignored events.

    rootWriter.restartRow();
    rootWriter.saveRow();
    rootWriter.endWrite();
  }
}
