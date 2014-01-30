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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.util.TestTools;
import org.apache.drill.jdbc.Driver;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.google.common.base.Stopwatch;

public class TestJdbcQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJdbcQuery.class);

  static final boolean IS_DEBUG = java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;

  // Set a timeout unless we're debugging.
  @Rule public TestRule TIMEOUT = TestTools.getTimeoutRule();

  static{
    Driver.load();
  }
  
  @Test 
  public void testCast() throws Exception{
    testQuery("select R_REGIONKEY, cast(R_NAME as varchar(15)) as region, cast(R_COMMENT as varchar(255)) as comment from parquet.`/Users/jnadeau/region.parquet`");    
  }
  @Test 
  public void testWildcard() throws Exception{
    testQuery("select * from parquet.`/Users/jnadeau/region.parquet`");
  }
  
  @Test 
  public void testLogicalExplain() throws Exception{
    testQuery("EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR select * from parquet.`/Users/jnadeau/region.parquet`");
  }

  @Test 
  public void testPhysicalExplain() throws Exception{
    testQuery("EXPLAIN PLAN FOR select * from parquet.`/Users/jnadeau/region.parquet`");
  }
  
  @Test 
  public void checkUnknownColumn() throws Exception{
    testQuery("select unknownColumn from parquet.`/Users/jnadeau/region.parquet`");
  }

  
  private void testQuery(String sql) throws Exception{
    boolean success = false;
    try (Connection c = DriverManager.getConnection("jdbc:drill:zk=local", null);) {
      for (int x = 0; x < 1; x++) {
        Stopwatch watch = new Stopwatch().start();
        Statement s = c.createStatement();
        ResultSet r = s.executeQuery(sql);
        boolean first = true;
        while (r.next()) {
          ResultSetMetaData md = r.getMetaData();
          if (first == true) {
            for (int i = 1; i <= md.getColumnCount(); i++) {
              System.out.print(md.getColumnName(i));
              System.out.print('\t');
            }
            System.out.println();
            first = false;
          }

          for (int i = 1; i <= md.getColumnCount(); i++) {
            System.out.print(r.getObject(i));
            System.out.print('\t');
          }
          System.out.println();
        }

        System.out.println(String.format("Query completed in %d millis.", watch.elapsed(TimeUnit.MILLISECONDS)));
      }

      success = true;
    }finally{
      if(!success) Thread.sleep(2000);
    }
    
    
  }
}
