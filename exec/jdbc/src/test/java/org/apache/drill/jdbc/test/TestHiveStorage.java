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
    testQuery("select * from hive.`default`.kv");
    testQuery("select key from hive.`default`.kv group by key");
    testQuery("select * from hive.`default`.allreadsupportedhivedatatypes");
  }

  @Test
  public void testHiveWithDate() throws Exception {
    testQuery("select * from hive.`default`.foodate");
    testQuery("select date_add(a, time '12:23:33'), b from hive.`default`.foodate");
  }

  @Test
  public void testQueryEmptyHiveTable() throws Exception {
    testQuery("SELECT * FROM hive.`default`.empty_table");
  }

}
