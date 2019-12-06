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
package org.apache.drill.exec.store.base;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;

import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for the Filter push-down helper classes as part of the
 * "Base" storage plugin to be used for add-on plugins. Uses a
 * "test mule" ("Dummy") plug-in which goes through the paces of
 * planning push down, but then glosses over the details at run time.
 * The focus here are plans: the tests plan a query then compare the
 * actual plan against and expected ("golden") plan.
 * <p>
 * If comparison fails, the tests print the actual plan to the console.
 * Use this, when adding new tests, to create the initial "golden" file.
 * Also, on failures, actual output is written to
 * <code>/tmp/drill/store/base</code>. You can use your IDE to compare
 * the actual and golden files to understand changes. If the changes
 * are expected, use that same IDE to copy changes from the actual
 * to the golden file.
 * <p>
 * The JSON properties of the serialized classes are all controlled
 * to have a fixed order to ensure that files compare across test
 * runs.
 */
public class TestFilterPushDown extends ClusterTest {

  private static final String BASE_SQL = "SELECT a, b FROM dummy.myTable";
  private static final String BASE_WHERE = BASE_SQL +  " WHERE ";

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher);
    startCluster(builder);

    StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();
    DummyStoragePluginConfig config =
        new DummyStoragePluginConfig(true, true, false);
    pluginRegistry.createOrUpdate("dummy", config, true);
  }

  //-------------------------------------------------
  // Unsupported filter push-down cases

  // No predicates

  @Test
  public void testNoPushDown() throws Exception
  {
    verifyPlan(BASE_SQL, "noPushDown.json");
  }

  // Predicate mismatch on type (id should be INT, dummy does
  // not try to convert)

  @Test
  public void testTypeMismatch() throws Exception
  {
    verifyPlan(BASE_WHERE + "id = 'foo'", "typeMismatch.json");
  }

  // Unsupported relop type (dummy supports limited set)

  @Test
  public void testUnsupportedOp() throws Exception
  {
    verifyPlan(BASE_WHERE + "a <> 'foo'", "unsupportedOp.json");
  }

  // Column reference rather than constant

  @Test
  public void testNonConst() throws Exception
  {
   verifyPlan(BASE_WHERE + "a = b", "nonConstPred.json");
  }

  // Unknown column (dummy only knows columns a and b)

  @Test
  public void testUnsupportedCol() throws Exception
  {
    verifyPlan(BASE_WHERE + "c = 'foo'", "unsupportedColPred.json");
  }

  // Not simple col = const

  @Test
  public void testComplexPred() throws Exception
  {
    verifyPlan(BASE_WHERE + "id + 10 = 20", "complexPred.json");
  }

  // Complex schema paths

  @Test
  public void testComplexCols() throws Exception
  {
    verifyPlan(BASE_WHERE + "a[10] = 'foo' AND myTable.b.c = 'bar'", "complexCols.json");
  }

  // OR, can't push

  @Test
  public void testGenericOr() throws Exception
  {
    verifyPlan(BASE_WHERE + "a = 'bar' OR id = 10", "or.json");
  }

  // Listener rejects one of the expressions within an OR,
  // must reject the entire OR clause. (Dummy rejects >.)

  @Test
  public void testRejectOneOrExpr() throws Exception
  {
    verifyPlan(BASE_WHERE + "a = 'bar' OR a > 'foo'", "rejectOneOrExpr.json");
  }

  // Or clause expressions accepted, but whole of OR is rejected
  // because it is not all = operators. (Dummy accepts <.)

  @Test
  public void testNonEqOr() throws Exception
  {
    verifyPlan(BASE_WHERE + "a = 'bar' OR a < 'foo'", "nonEqOr.json");
  }

  @Test
  public void testDoubleOr() throws Exception
  {
    verifyPlan(BASE_WHERE + "(a = 'x' OR a = 'y') AND (a = 'a' OR a = 'b')", "doubleOr.json");
  }

  //-------------------------------------------------
  // Supported filter push-down cases

  // Single matching predicate

  @Test
  public void testSingleCol() throws Exception
  {
    verifyPlan(BASE_WHERE + "a = 'bar'", "singleCol.json");
  }

    // Two matching predicates, one is implicit (not in project list)

  @Test
  public void testTwoCols() throws Exception
  {
    verifyPlan(BASE_WHERE + "a = 'bar' AND id = 10", "twoCols.json");
  }

  // Pushed and unpushed conditions

  @Test
  public void testMixedPreds() throws Exception
  {
    verifyPlan(BASE_WHERE + "a = 'bar' AND id = 10 AND c > 20", "mixedPreds.json");
  }

  // Reversed predicate

  @Test
  public void testReversed() throws Exception
  {
    verifyPlan(BASE_WHERE + "'bar' > a", "reversed.json");
  }

  // Join, same project list from both scans. (Calcite does filter push down
  // only once into a scan node shared by both scans.)

  @Test
  @Ignore("DRILL-7457")
  public void testSimpleJoin() throws Exception
  {
    String sql =
        "SELECT t1.a, t1.b, t2.b FROM dummy.myTable t1, dummy.myTable t2 WHERE t1.a = t2.a AND t1.a = 'bar'";
    verifyPlan(sql, "join1.json");
  }

  // Similar case, but different project lists. So, two scan nodes and two
  // sets of filter push downs. (Difference is: t2.b --> t2.c)

  @Test
  @Ignore("DRILL-7457")
  public void testComplexJoin() throws Exception
  {
    String sql =
        "SELECT t1.a, t1.b, t2.c FROM dummy.myTable t1, dummy.myTable t2 WHERE t1.a = t2.a AND t1.a = 'bar'";
    verifyPlan(sql, "join2.json");
  }

  // IS NULL and IS NOT NULL. Has only one argument.

  @Test
  public void testIsNull() throws Exception
  {
    String sql = "SELECT a, b FROM dummy.myTable WHERE a IS NULL AND b IS NOT NULL";
    verifyPlan(sql, "isNull.json");
  }

  // BETWEEN. Calcite rewrites a BETWEEN x AND y into
  // a >= x AND a <= y.
  // Since the dummy plug-in only handles <=, the => is left in the query.

  @Test
  public void testBetween() throws Exception
  {
    String sql = "SELECT a, b FROM dummy.myTable WHERE a BETWEEN 'bar' AND 'foo'";
    verifyPlan(sql, "between.json");
  }

  // IN clause, handled as a = 'bar' OR a = 'foo'.
  // Presumption is that this turns into multiple scans

  @Test
  public void testIn() throws Exception
  {
    String sql = "SELECT a, b FROM dummy.myTable WHERE a IN('bar', 'foo')";
    verifyPlan(sql, "in.json");
  }

  // Equivalent to the above

  @Test
  public void testInLikeOr() throws Exception
  {
    String sql = "SELECT a, b FROM dummy.myTable WHERE a = 'bar' OR a = 'foo'";
    verifyPlan(sql, "in.json");
  }

  // Test constant value conversion: Dummy will convert a to VARCHAR

  @Test
  public void testTypeConversion() throws Exception
  {
    verifyPlan(BASE_WHERE + "a = 10", "typeConversion.json");
  }

  public String basePath = "/store/base/";
  public boolean saveResults = true;

  protected void verifyPlan(String sql, String expectedFile) throws Exception {
    String plan = client.queryBuilder().sql(sql).explainJson();
    verify(plan, expectedFile);
  }

  protected void verify(String actual, String relativePath) {
    URL url = getClass().getResource(basePath + relativePath);
    if (url == null) {
      System.out.println(actual);
    }
    assertNotNull("Golden file is missing: " + relativePath, url);
    File resource = new File(url.getPath());
    try {
      verify(actual, resource);
    } catch (AssertionError e) {
      if (saveResults) {
        System.out.println(actual);
        File dest = new File("/tmp/drill", basePath);
        File destFile = new File(dest, relativePath);
        dest.mkdirs();
        try (PrintWriter out = new PrintWriter(destFile)) {
          out.println(actual);
        } catch (FileNotFoundException e1) {
          // Warn user, but don't fail test
          System.err.print("Cannnot save actual results to ");
          System.err.println(destFile.getAbsolutePath());
        }
      }
      throw e;
    }
  }

  protected void verify(String actual, File resource) {
    try (Reader expected = new FileReader(resource)) {
      verify(new StringReader(actual), expected);
    } catch (FileNotFoundException e) {
      fail("Missing resource file: " + resource.getAbsolutePath());
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  private void verify(Reader actualReader, Reader expectedReader) throws IOException {
    BufferedReader actual = new BufferedReader(actualReader);
    BufferedReader expected = new BufferedReader(expectedReader);
    for (;;) {
      String aLine = actual.readLine();
      String eLine = expected.readLine();
      if (aLine == null && eLine == null) {
        break;
      }
      if (eLine == null) {
        fail("Too many actual lines");
      }
      if (aLine == null) {
        fail("Missing actual lines");
      }
      assertEquals(eLine, aLine);
    }
  }
}

