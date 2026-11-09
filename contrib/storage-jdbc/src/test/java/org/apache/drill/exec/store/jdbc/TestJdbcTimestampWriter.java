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
package org.apache.drill.exec.store.jdbc;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetOptionBuilder;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.jdbc.writers.JdbcTimestampWriter;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Calendar;

/**
 * A JDBC TIMESTAMP has no zone, so a wall clock that does not exist in the session zone
 * (2:30 on the morning the clocks go forward) must still read back. Drivers throw
 * "HOUR_OF_DAY: 2 -&gt; 3" when asked to resolve one in a zone that has the gap.
 */
public class TestJdbcTimestampWriter extends SubOperatorTest {

  /** The wall clock stored in the column: a time that America/New_York skips. */
  private static final LocalDateTime SKIPPED = LocalDateTime.parse("2024-03-10T02:30:00");

  @Test
  public void readsAWallClockInsideADaylightSavingGap() throws Exception {
    TupleMetadata schema = new SchemaBuilder()
      .addNullable("ts", MinorType.TIMESTAMP)
      .buildSchema();
    ResultSetLoader loader = new ResultSetLoaderImpl(fixture.allocator(),
      new ResultSetOptionBuilder().readerSchema(schema).build());
    RowSetLoader rowWriter = loader.writer();
    JdbcTimestampWriter writer = new JdbcTimestampWriter("ts", rowWriter, 1);

    loader.startBatch();
    rowWriter.start();
    writer.load(gapUnfriendlyResultSet());
    rowWriter.save();

    RowSet expected = fixture.rowSetBuilder(schema)
      // The wall clock survives, rather than being shifted or dropped.
      .addRow(SKIPPED.toInstant(java.time.ZoneOffset.UTC).toEpochMilli())
      .build();
    RowSetUtilities.verify(expected, fixture.wrap(loader.harvest()));
  }

  /**
   * Stands in for a driver configured with a session zone that has the gap: it can only
   * hand the value over when the caller names a zone, the way MySQL Connector/J does.
   */
  private static ResultSet gapUnfriendlyResultSet() {
    return (ResultSet) Proxy.newProxyInstance(
      TestJdbcTimestampWriter.class.getClassLoader(),
      new Class<?>[] {ResultSet.class},
      (proxy, method, args) -> {
        if (!"getTimestamp".equals(method.getName())) {
          throw new UnsupportedOperationException(method.getName());
        }
        if (args.length < 2 || !(args[1] instanceof Calendar)) {
          throw new SQLException("HOUR_OF_DAY: 2 -> 3");
        }
        Calendar cal = (Calendar) args[1];
        return new Timestamp(SKIPPED.atZone(cal.getTimeZone().toZoneId()).toInstant().toEpochMilli());
      });
  }
}
