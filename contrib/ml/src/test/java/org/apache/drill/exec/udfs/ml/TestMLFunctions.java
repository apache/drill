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
package org.apache.drill.exec.udfs.ml;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestMLFunctions extends ClusterTest {

  private static final String MODEL = "dfs.root.`models/iris_rf.pmml`";

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("models/"));
  }

  @Test
  public void predictsAClassFromAPmmlModel() throws Exception {
    assertEquals("setosa", species("5.1, 1.4, 0.2"));
    assertEquals("versicolor", species("6.0, 4.0, 1.3"));
    assertEquals("virginica", species("6.5, 5.5, 2.0"));
  }

  private static String species(String features) throws Exception {
    return client.queryBuilder().sql("SELECT t.p.predicted_species AS s FROM (SELECT predict('"
      + MODEL + "', " + features + ") AS p FROM (VALUES(1))) t").singletonString();
  }

  @Test
  public void scoresSeveralRowsInOneQueryInOrder() throws Exception {
    String sql = "SELECT t.p.predicted_species AS s FROM (SELECT predict('" + MODEL
      + "', a, b, c) AS p FROM (VALUES(5.1, 1.4, 0.2), (6.0, 4.0, 1.3), (6.5, 5.5, 2.0)) AS v(a, b, c)) t";
    RowSet rs = client.queryBuilder().sql(sql).rowSet();
    RowSetReader r = rs.reader();
    String[] expected = {"setosa", "versicolor", "virginica"};
    for (String label : expected) {
      assertTrue(r.next());
      assertEquals(label, r.scalar("s").getString());
    }
    assertFalse(r.next());
    rs.clear();
  }

  @Test
  public void returnsProbabilitiesAlongsideThePrediction() throws Exception {
    String sql = "SELECT t.p.probability_setosa AS a, t.p.probability_versicolor AS b, "
      + "t.p.probability_virginica AS c FROM (SELECT predict('" + MODEL
      + "', 5.1, 1.4, 0.2) AS p FROM (VALUES(1))) t";
    RowSet rs = client.queryBuilder().sql(sql).rowSet();
    RowSetReader r = rs.reader();
    assertTrue(r.next());
    double sum = r.scalar("a").getDouble() + r.scalar("b").getDouble() + r.scalar("c").getDouble();
    assertTrue(r.scalar("a").getDouble() > r.scalar("b").getDouble());
    assertEquals(1.0, sum, 1e-6);
    rs.clear();
  }


  @Test
  public void aNullFeatureInANonFloatColumnIsAMissingValue() throws Exception {
    // An INT column: before NULLs were treated as missing values this threw a raw NPE.
    String sql = "SELECT predict('" + MODEL + "', CAST(NULL AS INT), 1.4, 0.2) AS p "
      + "FROM (VALUES(1))";
    RowSet rs = client.queryBuilder().sql(sql).rowSet();
    RowSetReader r = rs.reader();
    assertTrue(r.next());
    assertNotNull(r.column("p"));
    rs.clear();
  }

  @Test
  public void aNullFeatureInAFloatColumnIsAMissingValue() throws Exception {
    String sql = "SELECT predict('" + MODEL + "', CAST(NULL AS DOUBLE), 1.4, 0.2) AS p "
      + "FROM (VALUES(1))";
    RowSet rs = client.queryBuilder().sql(sql).rowSet();
    RowSetReader r = rs.reader();
    assertTrue(r.next());
    assertNotNull(r.column("p"));
    rs.clear();
  }

  @Test
  public void aModelDeclaringADoctypeFailsTheQuery() throws Exception {
    try {
      client.queryBuilder().sql("SELECT predict('dfs.root.`models/xxe.pmml`', 5.1, 1.4, 0.2) AS p "
        + "FROM (VALUES(1))").run();
      fail("expected the query to fail");
    } catch (UserException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("DOCTYPE"));
    }
  }

  @Test
  public void theWrongNumberOfFeaturesFailsTheQuery() throws Exception {
    try {
      client.queryBuilder().sql("SELECT predict('" + MODEL + "', 5.1, 1.4) AS p FROM (VALUES(1))").run();
      fail("expected the query to fail");
    } catch (UserException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("expecting 3 features"));
      assertTrue(e.getMessage(), e.getMessage().contains("provides 2"));
    }
  }

  @Test
  public void aMissingModelFailsTheQuery() throws Exception {
    try {
      client.queryBuilder().sql("SELECT predict('dfs.root.`models/nope.pmml`', 5.1, 1.4, 0.2) AS p "
        + "FROM (VALUES(1))").run();
      fail("expected the query to fail");
    } catch (UserException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("nope.pmml"));
      assertTrue(e.getMessage(), e.getMessage().contains("Unable to open model file"));
    }
  }
}
