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
package org.apache.drill.exec.store.parquet;

import java.math.BigDecimal;

import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.test.BaseTestQuery;
import org.junit.Test;

public class TestParquetComplex extends BaseTestQuery {

  private static final String DATAFILE = "cp.`store/parquet/complex/complex.parquet`";

  @Test
  public void sort() throws Exception {
    String query = String.format("select * from %s order by amount", DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline_sorted.json")
            .build()
            .run();
  }

  @Test
  public void topN() throws Exception {
    String query = String.format("select * from %s order by amount limit 5", DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline_sorted.json")
            .build()
            .run();
  }

  @Test
  public void hashJoin() throws Exception{
    String query = String.format("select t1.amount, t1.`date`, t1.marketing_info, t1.`time`, t1.trans_id, t1.trans_info, t1.user_info " +
            "from %s t1, %s t2 where t1.amount = t2.amount", DATAFILE, DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  public void mergeJoin() throws Exception{
    test("alter session set `planner.enable_hashjoin` = false");
    String query = String.format("select t1.amount, t1.`date`, t1.marketing_info, t1.`time`, t1.trans_id, t1.trans_info, t1.user_info " +
            "from %s t1, %s t2 where t1.amount = t2.amount", DATAFILE, DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  public void selectAllColumns() throws Exception {
    String query = String.format("select amount, `date`, marketing_info, `time`, trans_id, trans_info, user_info from %s", DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  public void selectMap() throws Exception {
    String query = "select marketing_info from cp.`store/parquet/complex/complex.parquet`";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline5.json")
            .build()
            .run();
  }

  @Test
  public void selectMapAndElements() throws Exception {
    String query = "select marketing_info, t.marketing_info.camp_id as camp_id, t.marketing_info.keywords[2] as keyword2 from cp.`store/parquet/complex/complex.parquet` t";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline6.json")
            .build()
            .run();
  }

  @Test
  public void selectMultiElements() throws Exception {
    String query = "select t.marketing_info.camp_id as camp_id, t.marketing_info.keywords as keywords from cp.`store/parquet/complex/complex.parquet` t";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline7.json")
            .build()
            .run();
  }

  @Test
  public void testStar() throws Exception {
    testBuilder()
            .sqlQuery("select * from cp.`store/parquet/complex/complex.parquet`")
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  public void missingColumnInMap() throws Exception {
    String query = "select t.trans_info.keywords as keywords from cp.`store/parquet/complex/complex.parquet` t";
    String[] columns = {"keywords"};
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline2.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test
  public void secondElementInMap() throws Exception {
    String query = String.format("select t.`marketing_info`.keywords as keywords from %s t", DATAFILE);
    String[] columns = {"keywords"};
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline3.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test
  public void elementsOfArray() throws Exception {
    String query = String.format("select t.`marketing_info`.keywords[0] as keyword0, t.`marketing_info`.keywords[2] as keyword2 from %s t", DATAFILE);
    String[] columns = {"keyword0", "keyword2"};
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline4.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test
  public void elementsOfArrayCaseInsensitive() throws Exception {
    String query = String.format("select t.`MARKETING_INFO`.keywords[0] as keyword0, t.`Marketing_Info`.Keywords[2] as keyword2 from %s t", DATAFILE);
    String[] columns = {"keyword0", "keyword2"};
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline4.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test //DRILL-3533
  public void notxistsField() throws Exception {
    String query = String.format("select t.`marketing_info`.notexists as notexists1,\n" +
                                        "t.`marketing_info`.camp_id as id,\n" +
                                        "t.`marketing_info.camp_id` as notexists2\n" +
                                  "from %s t", DATAFILE);
    String[] columns = {"notexists1", "id", "notexists2"};
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("store/parquet/complex/baseline8.json")
        .baselineColumns(columns)
        .build()
        .run();
  }

  public void testReadRepeatedDecimals() throws Exception {

    JsonStringArrayList<BigDecimal> ints = new JsonStringArrayList<>();
    ints.add(new BigDecimal("999999.999"));
    ints.add(new BigDecimal("-999999.999"));
    ints.add(new BigDecimal("0.000"));

    JsonStringArrayList<BigDecimal> longs = new JsonStringArrayList<>();
    longs.add(new BigDecimal("999999999.999999999"));
    longs.add(new BigDecimal("-999999999.999999999"));
    longs.add(new BigDecimal("0.000000000"));

    JsonStringArrayList<BigDecimal> fixedLen = new JsonStringArrayList<>();
    fixedLen.add(new BigDecimal("999999999999.999999"));
    fixedLen.add(new BigDecimal("-999999999999.999999"));
    fixedLen.add(new BigDecimal("0.000000"));

    testBuilder()
        .sqlQuery("select * from cp.`parquet/repeatedIntLondFixedLenBinaryDecimal.parquet`")
        .unOrdered()
        .baselineColumns("decimal_int32", "decimal_int64", "decimal_fixedLen", "decimal_binary")
        .baselineValues(ints, longs, fixedLen, fixedLen)
        .go();
  }
}
