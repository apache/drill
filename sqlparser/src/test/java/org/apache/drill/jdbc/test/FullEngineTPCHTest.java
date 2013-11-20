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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;


public class FullEngineTPCHTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FullEngineTPCHTest.class);

  // Determine if we are in Eclipse Debug mode.
  static final boolean IS_DEBUG = java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;

  // Set a timeout unless we're debugging.
  @Rule public TestRule globalTimeout = IS_DEBUG ? new TestName() : new Timeout(1000000);
  
  @Test
  @Ignore // since this is a specifically located file.
  public void fullSelectStarEngine() throws Exception {
    JdbcAssert.withFull("parquet-local")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql("select _MAP['d'] as d, _MAP['b'] as b from \"/tmp/parquet_test_file_many_types\" ").displayResults(50);
  }

  @Test
  //this is used to test func with null input. 
  public void testSimpleFuncArgValidator() throws Exception {
    JdbcAssert.withFull("parquet-local")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql(  "select 3 + cast(_MAP['NATIONKEY'] as numeric) FROM \"/Users/jni//work/tpc-h-parquet/nation\""
            ).displayResults(100);
  }

  @Test
  @Ignore
  public void test2TableJoinIOBE() throws Exception {
    JdbcAssert.withFull("parquet-local")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql( 
            "SELECT P.* " + 
            " FROM " +
            " ( SELECT _MAP['P_PARTKEY'] as P_PARTKEY, " + 
            "          _MAP['P_MFGR'] as P_MFGR " + 
            "   FROM \"/Users/jni//work/tpc-h-parquet/part\") P,"+
            " (SELECT _MAP['PS_PARTKEY'] AS PS_PARTKEY, " + 
            "         _MAP['PS_SUPPKEY'] AS PS_SUPPKEY " +
            "  FROM \"/Users/jni//work/tpc-h-parquet/partsupp\") PS " +
            " WHERE P.P_PARTKEY  = PS.PS_PARTKEY "+
            " LIMIT 100 "
            ).displayResults(100);
  }

  @Test
  @Ignore
  public void testAggOverEmptyBatch() throws Exception {
    JdbcAssert.withFull("parquet-local")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql(  "select SUM(cast(_MAP['N_NATIONKEY'] as numeric)) FROM \"/Users/jni//work/tpc-h-parquet/nation\" where 0=1"
            ).displayResults(100);
  }
  
  @Test
  @Ignore
  public void test3TableJoins() throws Exception {
    JdbcAssert.withFull("parquet-local")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql(
            "SELECT S.S_ACCTBAL, S.S_NAME " +
            "FROM " +
            "( SELECT _MAP['P_PARTKEY'] as P_PARTKEY, " +
            "         _MAP['P_MFGR'] as P_MFGR " + 
            "  FROM \"/Users/jni//work/tpc-h-parquet/part\") P, " +
            " ( SELECT _MAP['S_SUPPKEY'] AS S_SUPPKEY, "+
            "          _MAP['S_NATIONKEY'] AS S_NATIONKEY, " + 
            "          _MAP['S_ACCTBAL'] AS S_ACCTBAL, " +
            "          _MAP['S_NAME']  AS S_NAME, " +
            "          _MAP['S_ADDRESS'] AS S_ADDRESS, " +
            "          _MAP['S_PHONE'] AS S_PHONE, " + 
            "          _MAP['S_COMMENT'] AS S_COMMENT " + 
            "   FROM \"/Users/jni//work/tpc-h-parquet/supplier\") S, " +
            " (SELECT _MAP['PS_PARTKEY'] AS PS_PARTKEY, " +
            "         _MAP['PS_SUPPKEY'] AS PS_SUPPKEY " +
            " FROM \"/Users/jni//work/tpc-h-parquet/partsupp\") PS " +  
            " WHERE P.P_PARTKEY  = PS.PS_PARTKEY and " + 
            "      S.S_SUPPKEY = PS.PS_SUPPKEY " + 
            " LIMIT 100 "
            ).displayResults(100);
  }
  
  
  @Test
  @Ignore
  public void fullSelectStarEngine2() throws Exception {
    JdbcAssert.withFull("parquet-local")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql("SELECT _MAP['L_EXTENDEDPRICE'] AS EXTENDEDPRICE FROM \"/Users/jni/work/tpc-h-parquet/lineitem/part-m-00000.parquet\" " +
             " WHERE CAST(_MAP['L_RETURNFLAG'] as varchar) = 'N'").displayResults(50);
  }
  
  @Test
  @Ignore
  public void setCPRead() throws Exception {
    JdbcAssert.withFull("json-cp")
    // .sql("select cast(_MAP['red'] as bigint) + 1 as red_inc from donuts ")
        .sql("select * from \"department.json\" ").displayResults(50);
  }
}
