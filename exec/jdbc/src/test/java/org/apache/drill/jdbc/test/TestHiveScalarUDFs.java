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


import org.apache.drill.exec.store.hive.HiveTestDataGenerator;
import org.apache.drill.jdbc.JdbcTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestHiveScalarUDFs extends JdbcTest {

  @BeforeClass
  public static void generateHive() throws Exception{
    new HiveTestDataGenerator().generateTestData();
  }

  /** Test a hive function that implements the interface {@link org.apache.hadoop.hive.ql.exec.UDF}. */
  @Test
  @Ignore("relies on particular timezone")
  public void simpleUDF() throws Exception {
    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT " +
            "from_unixtime(1237573801) as unix_timestamp, " +
            "UDFDegrees(cast(26.89 as DOUBLE)) as degrees " +
            "FROM cp.`employee.json` LIMIT 1")
        .returns("unix_timestamp=2009-03-20 11:30:01; degrees=1540.6835111067835");
  }

  /** Test a hive function that implements the interface {@link org.apache.hadoop.hive.ql.udf.generic.GenericUDF}. */
  @Test
  public void simpleGenericUDF() throws Exception{
    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT CAST(" +
            "encode('text', 'UTF-8') " +
            "AS VARCHAR(5)) " +
            "FROM cp.`employee.json` LIMIT 1")
        .returns("EXPR$0=text");
  }
}
