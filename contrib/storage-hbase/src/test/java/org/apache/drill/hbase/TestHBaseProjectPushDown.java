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
package org.apache.drill.hbase;

import org.junit.Test;

public class TestHBaseProjectPushDown extends BaseHBaseTest {

  @Test
  public void testRowKeyPushDown() throws Exception{
    runSQLVerifyCount("SELECT\n"
        + "row_key\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName"
        , 6);
  }

  @Test
  public void testColumnWith1RowPushDown() throws Exception{
    runSQLVerifyCount("SELECT\n"
        + "f2['c7'] as `f[c7]`\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName"
        , 1);
  }

  @Test
  public void testRowKeyAndColumnPushDown() throws Exception{
    setColumnWidth(9);
    runSQLVerifyCount("SELECT\n"
        + "row_key, f['c1']*31 as `f[c1]*31`, f['c2'] as `f[c2]`, 5 as `5`, 'abc' as `'abc'`\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName"
        , 6);
  }

  @Test
  public void testColumnFamilyPushDown() throws Exception{
    setColumnWidth(74);
    runSQLVerifyCount("SELECT\n"
        + "f, f2\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName"
        , 6);
  }

}
