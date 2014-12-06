/**
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
package org.apache.drill.exec.fn.impl;

import org.apache.drill.BaseTestQuery;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

public class TestNewDateFunctions extends BaseTestQuery {
  DateTime date;
  DateTimeFormatter formatter;
  long unixTimeStamp = -1;

  @Test
  public void testUnixTimeStampForDate() throws Exception {
    formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    date = formatter.parseDateTime("2009-03-20 11:30:01");
    unixTimeStamp = date.getMillis() / 1000;
    testBuilder()
        .sqlQuery("select unix_timestamp('2009-03-20 11:30:01') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(unixTimeStamp)
        .build().run();

    date = formatter.parseDateTime("2014-08-09 05:15:06");
    unixTimeStamp = date.getMillis() / 1000;
    testBuilder()
        .sqlQuery("select unix_timestamp('2014-08-09 05:15:06') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(unixTimeStamp)
        .build().run();

    date = formatter.parseDateTime("1970-01-01 00:00:00");
    unixTimeStamp = date.getMillis() / 1000;
    testBuilder()
        .sqlQuery("select unix_timestamp('1970-01-01 00:00:00') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(unixTimeStamp)
        .build().run();
  }

  @Test
  public void testUnixTimeStampForDateWithPattern() throws Exception {
    formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    date = formatter.parseDateTime("2009-03-20 11:30:01.0");
    unixTimeStamp = date.getMillis() / 1000;

    testBuilder()
        .sqlQuery("select unix_timestamp('2009-03-20 11:30:01.0', 'yyyy-MM-dd HH:mm:ss.SSS') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(unixTimeStamp)
        .build().run();

    formatter = DateTimeFormat.forPattern("yyyy-MM-dd");
    date = formatter.parseDateTime("2009-03-20");
    unixTimeStamp = date.getMillis() / 1000;

    testBuilder()
        .sqlQuery("select unix_timestamp('2009-03-20', 'yyyy-MM-dd') from cp.`employee.json` limit 1")
        .ordered()
        .baselineColumns("EXPR$0")
        .baselineValues(unixTimeStamp)
        .build().run();
  }
}
