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

public class TestHBaseFilterPushDown extends BaseHBaseTest {

  @Test
  public void testFilterPushDownRowKeyEqual() throws Exception{
    runSQLVerifyCount("SELECT\n"
        + "  tableName.*\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "  WHERE tableName.row_key = 'b4'"
        , 1);
  }

  @Test
  public void testFilterPushDownRowKeyGreaterThan() throws Exception{
    runSQLVerifyCount("SELECT\n"
        + "  tableName.*\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "  WHERE tableName.row_key > 'b4'"
        , 2);
  }

  @Test
  public void testFilterPushDownRowKeyLessThanOrEqualTo() throws Exception{
    runSQLVerifyCount("SELECT\n"
        + "  tableName.*\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "  WHERE 'b4' >= tableName.row_key"
        , 4);
  }

}
