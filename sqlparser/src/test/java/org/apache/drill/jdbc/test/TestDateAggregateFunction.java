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
package org.apache.drill.jdbc.test;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.store.hive.HiveTestDataGenerator;
import org.apache.drill.jdbc.Driver;
import org.apache.drill.jdbc.JdbcTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;

public class TestDateAggregateFunction {

  public static final String WORKING_PATH;
  static{
    Driver.load();
    WORKING_PATH = Paths.get("").toAbsolutePath().toString();

  }
  @Test
  public void testDateAggFunction() throws Exception{
    String query = new String("SELECT max(cast(HIRE_DATE as date)) as MAX_DATE, min(cast(HIRE_DATE as date)) as MIN_DATE" +
        " FROM `employee.json`");

    JdbcAssert.withFull("cp")
        .sql(query)
        .returns(
                "MAX_DATE=1998-01-01; " +
                "MIN_DATE=1993-05-01\n"
        );
  }

  @Test
  public void testIntervalAggFunction() throws Exception{
    String query = new String("select max(date_diff(date'2014-5-2', cast(HIRE_DATE as date))) as MAX_DAYS,  min(date_diff(date'2014-5-2', cast(HIRE_DATE as date))) MIN_DAYS" +
        " FROM `employee.json`");

    JdbcAssert.withFull("cp")
        .sql(query)
        .returns(
            "MAX_DAYS=7671 days 0:0:0.0; " +
                "MIN_DAYS=5965 days 0:0:0.0\n"
        );
  }
}
