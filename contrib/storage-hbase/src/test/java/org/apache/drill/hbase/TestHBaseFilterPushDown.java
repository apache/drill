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

import java.util.List;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore // Need to find a way to pass zookeeper port to HBase storage plugin configuration before enabling this test
public class TestHBaseFilterPushDown extends BaseTestQuery {
  private static final String TABLE_NAME = "TestTable1";

  private static HBaseAdmin admin;
  private static Configuration conf = HBaseConfiguration.create();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    admin = new HBaseAdmin(conf);
    TestTableGenerator.generateHBaseTable(admin, TABLE_NAME, 2, 1000);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    System.out.println("HBaseStorageHandlerTest: tearDownAfterClass()");
    admin.disableTable(TABLE_NAME);
    admin.deleteTable(TABLE_NAME);
  }

  @Test
  public void testFilterPushDownRowKeyEqual() throws Exception{
    verify("SELECT\n"
        + "  tableName.*\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "  WHERE tableName.row_key = 'b4'"
        , 1);
  }

  @Test
  public void testFilterPushDownRowKeyGreaterThan() throws Exception{
    verify("SELECT\n"
        + "  tableName.*\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "  WHERE tableName.row_key > 'b4'"
        , 2);
  }

  @Test
  public void testFilterPushDownRowKeyLessThanOrEqualTo() throws Exception{
    verify("SELECT\n"
        + "  tableName.*\n"
        + "FROM\n"
        + "  hbase.`[TABLE_NAME]` tableName\n"
        + "  WHERE 'b4' >= tableName.row_key"
        , 4);
  }

  protected void verify(String sql, int expectedRowCount) throws Exception{
    sql = sql.replace("[TABLE_NAME]", TABLE_NAME);
    List<QueryResultBatch> results = testSqlWithResults(sql);

    int rowCount = 0;
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for(QueryResultBatch result : results){
      rowCount += result.getHeader().getRowCount();
      loader.load(result.getHeader().getDef(), result.getData());
      if (loader.getRecordCount() <= 0) {
        break;
      }
      VectorUtil.showVectorAccessibleContent(loader, 8);
      loader.clear();
      result.release();
    }
    System.out.println("Total record count: " + rowCount);
    Assert.assertEquals(expectedRowCount, rowCount);
  }

}
