/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.hive;

import org.junit.Test;

public class TestInfoSchemaOnHiveStorage extends HiveTestBase {

  @Test
  public void showTablesFromDb() throws Exception{
    testBuilder()
        .sqlQuery("SHOW TABLES FROM hive.`default`")
        .unOrdered()
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
        .baselineValues("hive.default", "partition_pruning_test")
        .baselineValues("hive.default", "readtest")
        .baselineValues("hive.default", "empty_table")
        .baselineValues("hive.default", "infoschematest")
        .baselineValues("hive.default", "hiveview")
        .baselineValues("hive.default", "kv")
        .go();

    testBuilder()
        .sqlQuery("SHOW TABLES IN hive.db1")
        .unOrdered()
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
        .baselineValues("hive.db1", "kv_db1")
        .go();
  }

  @Test
  public void showDatabases() throws Exception{
    testBuilder()
        .sqlQuery("SHOW DATABASES")
        .unOrdered()
        .baselineColumns("SCHEMA_NAME")
        .baselineValues("hive.default")
        .baselineValues("hive.db1")
        .baselineValues("dfs.default")
        .baselineValues("dfs.root")
        .baselineValues("dfs.tmp")
        .baselineValues("sys")
        .baselineValues("dfs_test.home")
        .baselineValues("dfs_test.default")
        .baselineValues("dfs_test.tmp")
        .baselineValues("cp.default")
        .baselineValues("INFORMATION_SCHEMA")
        .go();
  }

  @Test
  public void describeTableNullableColumns() throws Exception{
    testBuilder()
        .sqlQuery("DESCRIBE hive.`default`.kv")
        .unOrdered()
        .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
        .baselineValues("key", "INTEGER", "YES")
        .baselineValues("value", "VARCHAR", "YES")
        .go();
  }

  @Test
  public void varCharMaxLengthAndDecimalPrecisionInInfoSchema() throws Exception{
    final String query = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE " +
        "FROM INFORMATION_SCHEMA.`COLUMNS` " +
        "WHERE TABLE_SCHEMA = 'hive.default' AND TABLE_NAME = 'infoschematest' AND " +
        "(COLUMN_NAME = 'stringtype' OR COLUMN_NAME = 'varchartype' OR " +
        "COLUMN_NAME = 'inttype' OR COLUMN_NAME = 'decimaltype')";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE hive")
        .baselineColumns("COLUMN_NAME", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE")
        .baselineValues("inttype", "INTEGER", -1, -1, -1)
        .baselineValues("decimaltype", "DECIMAL", -1, 38, 2)
        .baselineValues("stringtype", "VARCHAR", 65535, -1, -1)
        .baselineValues("varchartype", "VARCHAR", 20, -1, -1)
        .go();
  }

  @Test
  public void defaultSchemaHive() throws Exception{
    testBuilder()
        .sqlQuery("SELECT * FROM kv LIMIT 2")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE hive")
        .baselineColumns("key", "value")
        .baselineValues(1, " key_1")
        .baselineValues(2, " key_2")
        .go();
  }

  @Test
  public void defaultTwoLevelSchemaHive() throws Exception{
    testBuilder()
        .sqlQuery("SELECT * FROM kv_db1 LIMIT 2")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE hive.db1")
        .baselineColumns("key", "value")
        .baselineValues(1, " key_1")
        .baselineValues(2, " key_2")
        .go();
  }
}
