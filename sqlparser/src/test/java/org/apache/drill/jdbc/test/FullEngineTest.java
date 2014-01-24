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

import org.junit.*;
                                                                                                                                import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.File;


public class FullEngineTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FullEngineTest.class);

  // Determine if we are in Eclipse Debug mode.
  static final boolean IS_DEBUG = java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;

  // Set a timeout unless we're debugging.
  @Rule public TestRule globalTimeout = IS_DEBUG ? new TestName() : new Timeout(100000);

  @Test
  @Ignore // since this is a specifically located file.
  public void fullSelectStarEngine() throws Exception {
    JdbcAssert.withFull("parquet-local")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql("select _MAP['d'] as d, _MAP['b'] as b from \"/tmp/parquet_test_file_many_types\" ").displayResults(50);
  }

  @Test
  public void setCPRead() throws Exception {
    JdbcAssert.withFull("json-cp")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql("select * from \"department.json\" ").displayResults(50);
  }

  /**
   * List tables using the system table (metadata.TABLES) which is same as "!tables" from SQLLine
   * @throws Exception
   */
  @Test(timeout=100000) // derby initialization is slow
  @Ignore
  public void listHiveTables() throws Exception {
    JdbcAssert.withFull("hive-derby")
      .sql("select * from \"metadata\".\"TABLES\"")
      .returns(
        "tableCat=null; tableSchem=hive-derby; tableName=testdb1.kv_seq; tableType=TABLE; remarks=null; typeCat=null; typeSchem=null; typeName=null; selfReferencingColName=null; refGeneration=null\n" +
        "tableCat=null; tableSchem=hive-derby; tableName=default.all_types; tableType=TABLE; remarks=null; typeCat=null; typeSchem=null; typeName=null; selfReferencingColName=null; refGeneration=null\n" +
        "tableCat=null; tableSchem=hive-derby; tableName=default.kv_text; tableType=TABLE; remarks=null; typeCat=null; typeSchem=null; typeName=null; selfReferencingColName=null; refGeneration=null\n" +
        "tableCat=null; tableSchem=metadata; tableName=COLUMNS; tableType=SYSTEM_TABLE; remarks=null; typeCat=null; typeSchem=null; typeName=null; selfReferencingColName=null; refGeneration=null\n" +
        "tableCat=null; tableSchem=metadata; tableName=TABLES; tableType=SYSTEM_TABLE; remarks=null; typeCat=null; typeSchem=null; typeName=null; selfReferencingColName=null; refGeneration=null\n"
      );
  }

  @Test(timeout=1000000) // derby initialization is slow
  @Ignore // ignore this until Hive SerDe/InputFormat work is intergrated
  public void selectFromHiveTable() throws Exception {
    JdbcAssert.withFull("hive-derby")
      .sql("select * from \"testdb1\".\"kv\"").displayResults(10);
  }

  @AfterClass
  public static void cleanup() {
    // derby creates a derby.log in cwd. Currently there seems to be no way to pass the config
    // property from hive component to derby to disable or write logging in a different location
    // need to delete this file for RAT pass
    File derbyLog = new File("./derby.log");
    if (derbyLog.exists()) {
      derbyLog.delete();
    }
  }
}
