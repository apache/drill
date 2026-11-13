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

package org.apache.drill.exec.store.jdbc.writers;

import org.apache.drill.exec.physical.resultSet.RowSetLoader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class JdbcTimestampWriter extends JdbcColumnWriter {

  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  /** Reused across rows: a scan reads its columns on one thread. */
  private final Calendar utc = new GregorianCalendar(UTC);

  public JdbcTimestampWriter(String colName, RowSetLoader rowWriter, int columnIndex) {
    super(colName, rowWriter, columnIndex);
  }

  @Override
  public void load(ResultSet results) throws SQLException {
    // A JDBC TIMESTAMP carries no zone, so read it as UTC rather than letting the driver
    // resolve it in the session zone: a wall clock inside a DST gap (2:30 on the morning
    // the clocks go forward) has no instant in a zone that has the gap, and drivers throw
    // ("HOUR_OF_DAY: 2 -> 3") instead of returning the row. UTC has no transitions.
    Timestamp value = results.getTimestamp(columnIndex, utc);
    if (value != null) {
      columnWriter.setTimestamp(value.toInstant());
    }
  }
}
