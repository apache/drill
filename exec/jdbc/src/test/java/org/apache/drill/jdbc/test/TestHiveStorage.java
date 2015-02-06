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
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHiveStorage extends JdbcTestQueryBase{

  @BeforeClass
  public static void generateHive() throws Exception{
    new HiveTestDataGenerator().generateTestData();
  }

  @Test
  public void testHiveReadWithDb() throws Exception{
    testQuery("select * from hive_test.`default`.kv");
    testQuery("select key from hive_test.`default`.kv group by key");
  }

  @Test
  public void testHiveWithDate() throws Exception {
    testQuery("select * from hive_test.`default`.foodate");
    testQuery("select date_add(a, time '12:23:33'), b from hive_test.`default`.foodate");
  }

  @Test
  public void testQueryEmptyHiveTable() throws Exception {
    testQuery("SELECT * FROM hive_test.`default`.empty_table");
  }

  @Test
  public void testReadAllSupportedHiveDataTypes() throws Exception {
    // There are known issues with displaying VarBinary in JDBC. So for now just execute the query and do not
    // verify the results until display issues with VarBinary are resolved.
    testQuery("SELECT * FROM hive_test.`default`.readtest");

    /*
    JdbcAssert.withFull("hive_test.`default`")
        .sql("SELECT * FROM readtest")
        .returns(
            "binary_field=[B@7005f08f; " + // know issues with binary display
            "boolean_field=false; " +
            "tinyint_field=34; " +
            "decimal_field=3489423929323435243; " +
            "double_field=8.345; " +
            "float_field=4.67; " +
            "int_field=123456; " +
            "bigint_field=234235; " +
            "smallint_field=3455; " +
            "string_field=stringfield; " +
            "varchar_field=varcharfield; " +
            "timestamp_field=2013-07-05T17:01:00.000-07:00; " +
            "date_field=2013-07-05T00:00:00.000-07:00; " +
            "binary_part=[B@7008383e; " + // know issues with binary display
            "boolean_part=true; " +
            "tinyint_part=64; " +
            "decimal_part=3489423929323435243; " +
            "double_part=8.345; " +
            "float_part=4.67; " +
            "int_part=123456; " +
            "bigint_part=234235; " +
            "smallint_part=3455; " +
            "string_part=string; " +
            "varchar_part=varchar; " +
            "timestamp_part=2013-07-05T17:01:00.000-07:00; " +
            "date_part=2013-07-05T00:00:00.000-07:00");
    */
  }

  @Test
  public void testOrderByOnHiveTable() throws Exception {
    JdbcAssert.withFull("hive_test.`default`")
        .sql("SELECT * FROM kv ORDER BY `value` DESC")
        .returns(
            "key=5; value= key_5\n" +
            "key=4; value= key_4\n" +
            "key=3; value= key_3\n" +
            "key=2; value= key_2\n" +
            "key=1; value= key_1\n"
        );
  }
}
