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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.protocol.BatchAccessor;
import org.apache.drill.exec.physical.impl.protocol.VectorContainerAccessor;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.ResultSetReader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl.ResultSetOptions;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;

import jersey.repackaged.com.google.common.base.Preconditions;

public class TestResultSetReader extends SubOperatorTest {

  public static class BatchGenerator {

    private enum State { SCHEMA1, SCHEMA2 };

    private final ResultSetLoader rsLoader;
    private final VectorContainerAccessor batch = new VectorContainerAccessor();
    private State state;

    public BatchGenerator() {
      TupleMetadata schema1 = new SchemaBuilder()
          .add("id", MinorType.INT)
          .add("name", MinorType.VARCHAR)
          .build();
      ResultSetOptions options = new OptionBuilder()
          .setSchema(schema1)
          .build();
      rsLoader = new ResultSetLoaderImpl(fixture.allocator(), options);
      state = State.SCHEMA1;
    }

    public BatchAccessor batch1(int start, int end) {
      Preconditions.checkState(state == State.SCHEMA1);
      rsLoader.startBatch();
      RowSetLoader writer = rsLoader.writer();
      for (int i = start; i <= end; i++) {
        writer.start();
        writer.scalar("id").setInt(i);
        writer.scalar("name").setString("Row" + i);
        writer.save();
      }
      batch.addBatch(rsLoader.harvest());
      return batch;
    }

    public BatchAccessor batch2(int start, int end) {
      RowSetLoader writer = rsLoader.writer();
      if (state == State.SCHEMA1) {
        ColumnMetadata balCol = MetadataUtils.newScalar("amount", MinorType.INT, DataMode.REQUIRED);
        writer.addColumn(balCol);
        state = State.SCHEMA2;
      }
      rsLoader.startBatch();
      for (int i = start; i <= end; i++) {
        writer.start();
        writer.scalar("id").setInt(i);
        writer.scalar("name").setString("Row" + i);
        writer.scalar("amount").setInt(i * 10);
        writer.save();
      }
      batch.addBatch(rsLoader.harvest());
      return batch;
    }

    public void close() {
      rsLoader.close();
    }
  }

  @Test
  public void testBasics() {
    ResultSetReader rsReader = new ResultSetReaderImpl();

    // Start state

    try {
      rsReader.reader();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsReader.detach();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      rsReader.release();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Make a batch. Verify reader is attached.
    // (Don't need to do a full reader test, that is already done
    // elsewhere.)

    BatchGenerator gen = new BatchGenerator();
    rsReader.start(gen.batch1(1, 10));
    RowSetReader reader1;
    {
      RowSetReader reader = rsReader.reader();
      reader1 = reader;
      assertTrue(reader.next());
      assertEquals(1, reader.scalar("id").getInt());
      assertEquals("Row1", reader.scalar("name").getString());
    }
    rsReader.release();
    try {
      rsReader.reader();
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }

    // Another batch of same schema

    rsReader.start(gen.batch1(11, 20));
    {
      RowSetReader reader = rsReader.reader();
      assertSame(reader1, reader);
      reader1 = reader;
      assertTrue(reader.next());
      assertEquals(11, reader.scalar("id").getInt());
      assertEquals("Row11", reader.scalar("name").getString());
    }
    rsReader.release();

    // Batch with new schema

    rsReader.start(gen.batch2(21, 30));
    {
      RowSetReader reader = rsReader.reader();
      assertNotSame(reader1, reader);
      reader1 = reader;
      assertTrue(reader.next());
      assertEquals(21, reader.scalar("id").getInt());
      assertEquals("Row21", reader.scalar("name").getString());
      assertEquals(210, reader.scalar("amount").getInt());
    }
    rsReader.release();

    rsReader.close();
  }

  @Test
  public void testCloseAtStart() {
    ResultSetReaderImpl rsReader = new ResultSetReaderImpl();

    // Close OK in start state

    rsReader.close();
    assertEquals(ResultSetReaderImpl.State.CLOSED, rsReader.state());

    // Second close OK

    rsReader.close();
  }

  @Test
  public void testAutoRelease() {
    ResultSetReader rsReader = new ResultSetReaderImpl(true);
    BatchGenerator gen = new BatchGenerator();
    rsReader.start(gen.batch1(1, 10));

    // If the test fails with open allocators, then the following failed.

    rsReader.close();
  }
}
