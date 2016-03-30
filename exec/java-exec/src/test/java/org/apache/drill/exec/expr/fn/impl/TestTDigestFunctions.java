/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestTDigestFunctions extends BaseTestQuery {

  private static final double MEDIAN = 34245.12;
  private static final double Q90 = 66633.84;
  private static final double Q99 = 86085.65;
  private static final double MEDIAN_TAX = 0.04;
  private static final double Q90_TAX = 0.08;
  private static final double MEDIAN_N_O = 34329.33;
  private static final double Q90_N_O = 66654.52;
  private static final double MEDIAN_N_F = 33298.92;
  private static final double Q90_N_F = 67175.52;
  private static final double MEDIAN_A_F = 34129.44;
  private static final double Q90_A_F = 66935.95;
  private static final double MEDIAN_R_F = 34245.12;
  private static final double Q90_R_F = 66172.05;

  @Test
  public void median() throws Exception {
    String query = "select median(l_extendedprice) m from cp.`tpch/lineitem.parquet`";
    Map<String,Float> tolerances = new HashMap<>();
    tolerances.put("`m`", new Float(1e-3));
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("m")
            .baselineValues(MEDIAN)
            .baselineTolerances(tolerances)
            .go();
  }

  @Test
  public void q90() throws Exception {
    String query = "select quantile(0.9, l_extendedprice) q90 from cp.`tpch/lineitem.parquet`";
    Map<String,Float> tolerances = new HashMap<>();
    tolerances.put("`q90`", new Float(1e-3));
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("q90")
            .baselineValues(Q90)
            .baselineTolerances(tolerances)
            .go();
  }

  @Test
  public void q99() throws Exception {
    String query = "select quantile(0.99, l_extendedprice) q90 from cp.`tpch/lineitem.parquet`";
    Map<String,Float> tolerances = new HashMap<>();
    tolerances.put("`q90`", new Float(1e-3));
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("q90")
            .baselineValues(Q99)
            .baselineTolerances(tolerances)
            .go();
  }

  @Test
  public void multipleInputs() throws Exception {
    String query = "select median(l_extendedprice) m, quantile(0.9, l_extendedprice) q90, median(l_tax) m_tax, quantile(0.9, l_tax) q90_tax from cp.`tpch/lineitem.parquet`";
    Map<String,Float> tolerances = new HashMap<>();
    tolerances.put("`m`", new Float(1e-3));
    tolerances.put("`q90`", new Float(1e-3));
    tolerances.put("`m_tax`", new Float(1e-3));
    tolerances.put("`q90_tax`", new Float(1e-3));
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("m", "q90", "m_tax", "q90_tax")
            .baselineValues(MEDIAN, Q90, MEDIAN_TAX, Q90_TAX)
            .baselineTolerances(tolerances)
            .go();
  }

  @Test
  public void groupBy() throws Exception {
    String query = "select median(l_extendedprice) m, quantile(0.9, l_extendedprice) q90 from cp.`tpch/lineitem.parquet` group by l_returnflag, l_linestatus";
    test(query);
    Map<String,Float> tolerances = new HashMap<>();
    tolerances.put("`m`", new Float(1e-2));
    tolerances.put("`q90`", new Float(1e-2));
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .baselineColumns("m", "q90")
            .baselineValues(MEDIAN_N_O, Q90_N_O)
            .baselineValues(MEDIAN_N_F, Q90_N_F)
            .baselineValues(MEDIAN_A_F, Q90_A_F)
            .baselineValues(MEDIAN_R_F, Q90_R_F)
            .baselineTolerances(tolerances)
            .go();
  }
}
