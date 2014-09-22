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
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.util.TestTools;
import org.apache.drill.jdbc.Driver;
import org.apache.drill.jdbc.JdbcTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

public class TestJdbcDistQuery extends JdbcTest{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJdbcDistQuery.class);


  // Set a timeout unless we're debugging.
  @Rule public TestRule TIMEOUT = TestTools.getTimeoutRule(50000);

  private static final String WORKING_PATH;
  static{
    Driver.load();
    WORKING_PATH = Paths.get("").toAbsolutePath().toString();

  }

  @Test
  public void testSimpleQuerySingleFile() throws Exception{
    testQuery(String.format("select R_REGIONKEY, R_NAME "
        + "from dfs_test.`%s/../../sample-data/regionsSF/`", WORKING_PATH));
  }


  @Test
  public void testSimpleQueryMultiFile() throws Exception{
    testQuery(String.format("select R_REGIONKEY, R_NAME "
        + "from dfs_test.`%s/../../sample-data/regionsMF/`", WORKING_PATH));
  }

  @Test
  public void testWhereOverSFile() throws Exception{
    testQuery(String.format("select R_REGIONKEY, R_NAME "
        + "from dfs_test.`%s/../../sample-data/regionsSF/` "
        + "WHERE R_REGIONKEY = 1", WORKING_PATH));
  }

  @Test
  public void testWhereOverMFile() throws Exception{
    testQuery(String.format("select R_REGIONKEY, R_NAME "
        + "from dfs_test.`%s/../../sample-data/regionsMF/` "
        + "WHERE R_REGIONKEY = 1", WORKING_PATH));
  }


  @Test
  public void testAggSingleFile() throws Exception{
    testQuery(String.format("select R_REGIONKEY "
        + "from dfs_test.`%s/../../sample-data/regionsSF/` "
        + "group by R_REGIONKEY", WORKING_PATH));
  }

  @Test
  public void testAggMultiFile() throws Exception{
    testQuery(String.format("select R_REGIONKEY "
        + "from dfs_test.`%s/../../sample-data/regionsMF/` "
        + "group by R_REGIONKEY", WORKING_PATH));
  }

  @Test
  public void testAggOrderByDiffGKeyMultiFile() throws Exception{
    testQuery(String.format("select R_REGIONKEY, SUM(cast(R_REGIONKEY AS int)) As S "
        + "from dfs_test.`%s/../../sample-data/regionsMF/` "
        + "group by R_REGIONKEY ORDER BY S", WORKING_PATH));
  }

  @Test
  public void testAggOrderBySameGKeyMultiFile() throws Exception{
    testQuery(String.format("select R_REGIONKEY, SUM(cast(R_REGIONKEY AS int)) As S "
        + "from dfs_test.`%s/../../sample-data/regionsMF/` "
        + "group by R_REGIONKEY "
        + "ORDER BY R_REGIONKEY", WORKING_PATH));
  }

  @Ignore
  @Test
  public void testJoinSingleFile() throws Exception{
    testQuery(String.format("select T1.R_REGIONKEY "
        + "from dfs_test.`%s/../../sample-data/regionsSF/` as T1 "
        + "join dfs_test.`%s/../../sample-data/nationsSF/` as T2 "
        + "on T1.R_REGIONKEY = T2.N_REGIONKEY", WORKING_PATH, WORKING_PATH));
  }

  @Ignore
  @Test
  public void testJoinMultiFile() throws Exception{
    testQuery(String.format("select T1.R_REGIONKEY "
        + "from dfs_test.`%s/../../sample-data/regionsMF/` as T1 "
        + "join dfs_test.`%s/../../sample-data/nationsMF/` as T2 "
        + "on T1.R_REGIONKEY = T2.N_REGIONKEY", WORKING_PATH, WORKING_PATH));
  }

  @Ignore
  @Test
  public void testJoinMFileWhere() throws Exception{
    testQuery(String.format("select T1.R_REGIONKEY, T1.R_NAME "
        + "from dfs_test.`%s/../../sample-data/regionsMF/` as T1 "
        + "join dfs_test.`%s/../../sample-data/nationsMF/` as T2 "
        + "on T1.R_REGIONKEY = T2.N_REGIONKEY "
        + "WHERE T1.R_REGIONKEY  = 3 ", WORKING_PATH, WORKING_PATH));
  }

  @Test
  //NPE at ExternalSortBatch.java : 151
  public void testSortSingleFile() throws Exception{
    testQuery(String.format("select R_REGIONKEY "
        + "from dfs_test.`%s/../../sample-data/regionsSF/` "
        + "order by R_REGIONKEY", WORKING_PATH));
  }

  @Test
  //NPE at ExternalSortBatch.java : 151
  public void testSortMultiFile() throws Exception{
    testQuery(String.format("select R_REGIONKEY "
        + "from dfs_test.`%s/../../sample-data/regionsMF/` "
        + "order by R_REGIONKEY", WORKING_PATH));
  }

  @Test
  public void testSortMFileWhere() throws Exception{
    testQuery(String.format("select R_REGIONKEY "
        + "from dfs_test.`%s/../../sample-data/regionsMF/` "
        + "WHERE R_REGIONKEY = 1 "
        + "order by R_REGIONKEY ", WORKING_PATH ));
  }

  @Ignore
  @Test
  public void testJoinAggSortWhere() throws Exception{
    testQuery(String.format("select T1.R_REGIONKEY, COUNT(1) as CNT "
        + "from dfs_test.`%s/../../sample-data/regionsMF/` as T1 "
        + "join dfs_test.`%s/../../sample-data/nationsMF/` as T2 "
        + "on T1.R_REGIONKEY = T2.N_REGIONKEY "
        + "WHERE T1.R_REGIONKEY  = 3 "
        + "GROUP BY T1.R_REGIONKEY "
        + "ORDER BY T1.R_REGIONKEY",WORKING_PATH, WORKING_PATH ));
  }

  @Test
  public void testSelectLimit() throws Exception{
    testQuery(String.format("select R_REGIONKEY, R_NAME "
        + "from dfs_test.`%s/../../sample-data/regionsMF/` "
        + "limit 2", WORKING_PATH));
  }

 private void testQuery(String sql) throws Exception{
    boolean success = false;
    try (Connection c = DriverManager.getConnection("jdbc:drill:zk=local", null);) {
      for (int x = 0; x < 1; x++) {
        Stopwatch watch = new Stopwatch().start();
        Statement s = c.createStatement();
        ResultSet r = s.executeQuery(sql);
        boolean first = true;
        ResultSetMetaData md = r.getMetaData();
        if (first == true) {
          for (int i = 1; i <= md.getColumnCount(); i++) {
            System.out.print(md.getColumnName(i));
            System.out.print('\t');
          }
          System.out.println();
          first = false;
        }
        while (r.next()) {
          md = r.getMetaData();

          for (int i = 1; i <= md.getColumnCount(); i++) {
            System.out.print(r.getObject(i));
            System.out.print('\t');
          }
          System.out.println();
        }

        System.out.println(String.format("Query completed in %d millis.", watch.elapsedMillis()));
      }

      System.out.println("\n\n\n");
      success = true;
    } finally {
      if (!success) {
        Thread.sleep(2000);
      }
    }
  }

  @Test
  public void testSchemaForEmptyResultSet() throws Exception {
    String query = "select fullname, occupation, postal_code from cp.`customer.json` where 0 = 1";
    try (Connection c = DriverManager.getConnection("jdbc:drill:zk=local", null);) {
      Statement s = c.createStatement();
      ResultSet r = s.executeQuery(query);
      ResultSetMetaData md = r.getMetaData();
      List<String> columns = Lists.newArrayList();
      for (int i = 1; i <= md.getColumnCount(); i++) {
        System.out.print(md.getColumnName(i));
        System.out.print('\t');
        columns.add(md.getColumnName(i));
      }
      String[] expected = {"fullname", "occupation", "postal_code"};
      Assert.assertEquals(3, md.getColumnCount());
      Assert.assertArrayEquals(expected, columns.toArray());
    }
  }

}
