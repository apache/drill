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
package org.apache.drill.exec.hive.complex_types;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.drill.categories.HiveStorageTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.hive.HiveTestBase;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.drill.test.TestBuilder.listOf;
import static org.apache.drill.test.TestBuilder.mapOf;
import static org.apache.drill.test.TestBuilder.mapOfObject;

@Category({SlowTest.class, HiveStorageTest.class})
public class TestHiveUnions extends HiveTestBase {

  @BeforeClass
  public static void generateTestData() throws Exception {
    String jdbcUrl = String.format("jdbc:hive2://%s:%d/default",
        HIVE_CONTAINER.getHost(),
        HIVE_CONTAINER.getMappedPort(10000));

    try (Connection conn = DriverManager.getConnection(jdbcUrl, "", "");
         Statement stmt = conn.createStatement()) {

      // Create dummy table for data generation
      stmt.execute("CREATE TABLE IF NOT EXISTS dummy(d INT) STORED AS TEXTFILE");
      stmt.execute("INSERT INTO TABLE dummy VALUES (1)");

      // Create union table
      String unionDdl = "CREATE TABLE IF NOT EXISTS union_tbl(" +
          "tag INT, " +
          "ut UNIONTYPE<INT, DOUBLE, ARRAY<STRING>, STRUCT<a:INT,b:STRING>, DATE, BOOLEAN," +
          "DECIMAL(9,3), TIMESTAMP, BIGINT, FLOAT, MAP<INT, BOOLEAN>, ARRAY<INT>>) " +
          "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' COLLECTION ITEMS TERMINATED BY '&' " +
          "MAP KEYS TERMINATED BY '#' LINES TERMINATED BY '\\n' STORED AS TEXTFILE";
      stmt.execute(unionDdl);

      // Insert test data for each union variant
      // The create_union function takes: (tag, v0, v1, v2, v3, ...)
      // and returns the variant at position 'tag'

      String insertTemplate = "INSERT INTO TABLE union_tbl " +
          "SELECT %1$d, " +
          "create_union(%1$d, " +
          "1, " +  // tag 0: INT
          "CAST(17.55 AS DOUBLE), " +  // tag 1: DOUBLE
          "array('x','yy','zzz'), " +  // tag 2: ARRAY<STRING>
          "named_struct('a',1,'b','x'), " +  // tag 3: STRUCT
          "CAST('2019-09-09' AS DATE), " +  // tag 4: DATE
          "true, " +  // tag 5: BOOLEAN
          "CAST(12356.123 AS DECIMAL(9,3)), " +  // tag 6: DECIMAL
          "CAST('2018-10-21 04:51:36' AS TIMESTAMP), " +  // tag 7: TIMESTAMP
          "CAST(9223372036854775807 AS BIGINT), " +  // tag 8: BIGINT
          "CAST(-32.058 AS FLOAT), " +  // tag 9: FLOAT
          "map(1,true,2,false,3,false,4,true), " +  // tag 10: MAP
          "array(7,-9,2,-5,22)" +  // tag 11: ARRAY<INT>
          ") FROM dummy";

      // Insert each union variant
      int[] tags = {1, 5, 0, 2, 4, 3, 11, 8, 7, 9, 10, 6};
      for (int tag : tags) {
        stmt.execute(String.format(insertTemplate, tag));
      }
    }
  }

  @Test
  public void checkUnion() throws Exception {
    testBuilder()
        .sqlQuery("SELECT tag, ut FROM hive.union_tbl")
        .unOrdered()
        .baselineColumns("tag", "ut")
        .baselineValues(1, 17.55)
        .baselineValues(5, true)
        .baselineValues(0, 1)
        .baselineValues(2, listOf("x", "yy", "zzz"))
        .baselineValues(4, DateUtility.parseLocalDate("2019-09-09"))
        .baselineValues(3, mapOf("a", 1, "b", "x"))
        .baselineValues(11, listOf(7, -9, 2, -5, 22))
        .baselineValues(8, 9223372036854775807L)
        .baselineValues(7, DateUtility.parseBest("2018-10-21 04:51:36"))
        .baselineValues(9, -32.058f)
        .baselineValues(10, mapOfObject(1, true, 2, false, 3, false, 4, true))
        .baselineValues(6, new BigDecimal("12356.123"))
        .go();
  }
}
