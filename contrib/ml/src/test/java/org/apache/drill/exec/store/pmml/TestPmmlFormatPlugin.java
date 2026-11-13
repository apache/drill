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
package org.apache.drill.exec.store.pmml;

import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestPmmlFormatPlugin extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("models/"));
  }

  @Test
  public void readsOneRowDescribingTheModel() throws Exception {
    RowSet results = client.queryBuilder()
      .sql("SELECT classification, clustering, regression FROM dfs.root.`models/iris_rf.pmml`")
      .rowSet();
    RowSetReader r = results.reader();
    assertTrue(r.next());
    assertTrue("iris_rf.pmml is a classifier", r.column("classification").scalar().getBoolean());
    assertFalse(r.column("clustering").scalar().getBoolean());
    assertFalse(r.column("regression").scalar().getBoolean());
    assertFalse("exactly one row per model file", r.next());
    results.clear();
  }

  @Test
  public void reportsTheInputFieldsInModelOrder() throws Exception {
    RowSet results = client.queryBuilder()
      .sql("SELECT input_fields FROM dfs.root.`models/iris_rf.pmml`")
      .rowSet();
    RowSetReader r = results.reader();
    assertTrue(r.next());
    // predict() takes its features positionally, so the order here is the order to pass them in.
    // The fixture's three active fields are sepal_length, petal_length, petal_width, in that order.
    String fields = r.column("input_fields").getAsString();
    int sepal = fields.indexOf("sepal_length");
    int petalLength = fields.indexOf("petal_length");
    int petalWidth = fields.indexOf("petal_width");
    assertTrue(fields, sepal >= 0 && petalLength > sepal && petalWidth > petalLength);
    assertFalse("the target field is not an input", fields.contains("species"));
    results.clear();
  }

  @Test
  public void reportsThePredictionClasses() throws Exception {
    RowSet results = client.queryBuilder()
      .sql("SELECT prediction_classes FROM dfs.root.`models/iris_rf.pmml`")
      .rowSet();
    RowSetReader r = results.reader();
    assertTrue(r.next());
    String classes = r.column("prediction_classes").getAsString();
    assertTrue(classes, classes.contains("setosa"));
    assertTrue(classes, classes.contains("versicolor"));
    assertTrue(classes, classes.contains("virginica"));
    results.clear();
  }

  @Test
  public void reportsEachInputFieldWithItsType() throws Exception {
    RowSet results = client.queryBuilder()
      .sql("SELECT input_schema FROM dfs.root.`models/iris_rf.pmml`")
      .rowSet();
    RowSetReader r = results.reader();
    assertTrue(r.next());
    String schema = r.column("input_schema").getAsString();
    assertTrue(schema, schema.contains("petal_length"));
    assertTrue(schema, schema.contains("double"));
    results.clear();
  }

  @Test
  public void aModelDeclaringADoctypeIsRefused() throws Exception {
    try {
      client.queryBuilder()
        .sql("SELECT classification FROM dfs.root.`models/xxe.pmml`")
        .rowSet();
      fail("a DOCTYPE-bearing model must be refused by the format plugin too");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("DOCTYPE"));
    }
  }

  @Test
  public void aFileThatIsNotAModelFailsCleanly() throws Exception {
    try {
      client.queryBuilder()
        .sql("SELECT classification FROM dfs.root.`models/corrupt.pmml`")
        .rowSet();
      fail("a corrupt model must fail with a read error, not an internal error");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains("as a PMML model"));
    }
  }
}
