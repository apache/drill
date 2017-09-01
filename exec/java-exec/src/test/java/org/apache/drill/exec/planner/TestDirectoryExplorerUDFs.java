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
package org.apache.drill.exec.planner;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.drill.PlanTestBase;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.fn.interp.TestConstantFolding;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.util.TestUtilities;
import org.apache.drill.exec.util.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class TestDirectoryExplorerUDFs extends PlanTestBase {

  private static class ConstantFoldingTestConfig {
    String funcName;
    String expectedFolderName;
    public ConstantFoldingTestConfig(String funcName, String expectedFolderName) {
      this.funcName = funcName;
      this.expectedFolderName = expectedFolderName;
    }
  }

  private static List<ConstantFoldingTestConfig> tests;
  private String path;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void init() {
    // Need the suffixes to make the names unique in the directory.
    // The capitalized name is on the opposite function (imaxdir and mindir)
    // because they are looking on opposite ends of the list.
    //
    // BIGFILE_2 with the capital letter at the start of the name comes
    // first in the case-sensitive ordering.
    // SMALLFILE_2 comes last in a case-insensitive ordering because it has
    // a suffix not found on smallfile.
    tests = ImmutableList.<ConstantFoldingTestConfig>builder()
        .add(new ConstantFoldingTestConfig("MAXDIR", "smallfile"))
        .add(new ConstantFoldingTestConfig("IMAXDIR", "SMALLFILE_2"))
        .add(new ConstantFoldingTestConfig("MINDIR", "BIGFILE_2"))
        .add(new ConstantFoldingTestConfig("IMINDIR", "bigfile"))
        .build();
  }

  @Before
  public void setup() throws Exception {
    new TestConstantFolding.SmallFileCreator(folder).createFiles(1, 1000);
    path = folder.getRoot().toPath().toString();
  }


  @Test
  public void testConstExprFolding_maxDir0() throws Exception {

    test("use dfs.root");

    List<String> allFiles = ImmutableList.<String>builder()
        .add("smallfile")
        .add("SMALLFILE_2")
        .add("bigfile")
        .add("BIGFILE_2")
        .build();

    String query = "select * from dfs.`" + path + "/*/*.csv` where dir0 = %s('dfs.root','" + path + "')";
    for (ConstantFoldingTestConfig config : tests) {
      // make all of the other folders unexpected patterns, except for the one expected in this case
      List<String> excludedPatterns = Lists.newArrayList();
      excludedPatterns.addAll(allFiles);
      excludedPatterns.remove(config.expectedFolderName);
      // The list is easier to construct programmatically, but the API below takes an array to make it easier
      // to write a list as a literal array in a typical test definition
      String[] excludedArray = new String[excludedPatterns.size()];

      testPlanMatchingPatterns(
          String.format(query, config.funcName),
          new String[] {config.expectedFolderName},
          excludedPatterns.toArray(excludedArray));
    }

    JsonStringArrayList<Text> list = new JsonStringArrayList<>();

    list.add(new Text("1"));
    list.add(new Text("2"));
    list.add(new Text("3"));

    testBuilder()
        .sqlQuery(String.format(query, tests.get(0).funcName))
        .unOrdered()
        .baselineColumns("columns", "dir0")
        .baselineValues(list, tests.get(0).expectedFolderName)
        .go();
  }

  @Test
  public void testIncorrectFunctionPlacement() throws Exception {

    Map<String, String> configMap = ImmutableMap.<String, String>builder()
        .put("select %s('dfs.root','" + path + "') from dfs.`" + path + "/*/*.csv`", "Select List")
        .put("select dir0 from dfs.`" + path + "/*/*.csv` order by %s('dfs.root','" + path + "')", "Order By")
        .put("select max(dir0) from dfs.`" + path + "/*/*.csv` group by %s('dfs.root','" + path + "')", "Group By")
        .put("select concat(concat(%s('dfs.root','" + path + "'),'someName'),'someName') from dfs.`" + path + "/*/*.csv`", "Select List")
        .put("select dir0 from dfs.`" + path + "/*/*.csv` order by concat(%s('dfs.root','" + path + "'),'someName')", "Order By")
        .put("select max(dir0) from dfs.`" + path + "/*/*.csv` group by concat(%s('dfs.root','" + path + "'),'someName')", "Group By")
        .build();

    for (Map.Entry<String, String> configEntry : configMap.entrySet()) {
      for (ConstantFoldingTestConfig functionConfig : tests) {
        try {
          test(String.format(configEntry.getKey(), functionConfig.funcName));
        } catch (UserRemoteException e) {
          assertThat(e.getMessage(), containsString(
              String.format("Directory explorers [MAXDIR, IMAXDIR, MINDIR, IMINDIR] functions are not supported in %s", configEntry.getValue())));
        }
      }
    }
  }

  @Test
  public void testConstantFoldingOff() throws Exception {
    try {
      test("set `planner.enable_constant_folding` = false;");
      String query = "select * from dfs.`" + path + "/*/*.csv` where dir0 = %s('dfs.root','" + path + "')";
      for (ConstantFoldingTestConfig config : tests) {
        try {
          test(String.format(query, config.funcName));
        } catch (UserRemoteException e) {
          assertThat(e.getMessage(), containsString("Directory explorers [MAXDIR, IMAXDIR, MINDIR, IMINDIR] functions can not be used " +
              "when planner.enable_constant_folding option is set to false"));
        }
      }
    } finally {
      test("set `planner.enable_constant_folding` = true;");
    }
  }

  @Test
  public void testOneArgQueryDirFunctions() throws Exception {
    //Initially update the location of dfs_test.tmp workspace with "path" temp directory just for use in this UTest
    final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    try {
      TestUtilities.updateDfsTestTmpSchemaLocation(pluginRegistry, path);

      //Results comparison of using Query Directory Functions (MAXDIR, IMAXDIR, MINDIR, IMINDIR) with one and two arguments
      String queryWithTwoArgFunc = "select * from dfs.`" + path + "/*/*.csv` where dir0 = %s('dfs.root','" + path + "')";
      String queryWithOneArgFunc = "select * from dfs.`" + path + "/*/*.csv` where dir0 = %s('dfs_test.tmp')";
      for (ConstantFoldingTestConfig config : tests) {
        testBuilder()
            .sqlQuery(String.format(queryWithOneArgFunc, config.funcName))
            .unOrdered()
            .sqlBaselineQuery(String.format(queryWithTwoArgFunc, config.funcName))
            .go();
      }
    } finally {
        TestUtilities.updateDfsTestTmpSchemaLocation(pluginRegistry, getDfsTestTmpSchemaLocation());
    }
  }

  @Test // DRILL-4720
  public void testDirectoryUDFsWithAndWithoutMetadataCache() throws Exception {
    FileSystem fs = getLocalFileSystem();
    // prepare test table with partitions
    Path table = new Path(getTempDir("table_with_partitions"));
    String tablePath = table.toUri().getPath();
    Path dataFile = new Path(TestTools.getWorkingPath(),"src/test/resources/parquet/alltypes_required.parquet");
    createPartitions(fs, table, dataFile, 2);

    Map<String, String> configurations = ImmutableMap.<String, String>builder()
        .put("mindir", "part_1")
        .put("imindir", "part_1")
        .put("maxdir", "part_2")
        .put("imaxdir", "part_2")
        .build();

    String query = "select dir0 from dfs.`%s` where dir0 = %s('dfs', '%s') limit 1";

    // run tests without metadata cache
    for (Map.Entry<String, String> entry : configurations.entrySet()) {
      testBuilder()
          .sqlQuery(query, tablePath, entry.getKey(), tablePath)
          .unOrdered()
          .baselineColumns("dir0")
          .baselineValues(entry.getValue())
          .go();
    }

    // generate metadata
    test("refresh table metadata dfs.`%s`", tablePath);

    // run tests with metadata cache
    for (Map.Entry<String, String> entry : configurations.entrySet()) {
      testBuilder()
          .sqlQuery(query, tablePath, entry.getKey(), tablePath)
          .unOrdered()
          .baselineColumns("dir0")
          .baselineValues(entry.getValue())
          .go();
    }
  }

  private void createPartitions(FileSystem fs, Path table, Path dataFile, int number) throws IOException {
    for (int i = 1; i <= number; i++) {
      Path partition = new Path(table, "part_" + i);
      fs.mkdirs(partition);
      FileUtil.copy(fs, dataFile, fs, partition, false, true, fs.getConf());
    }
  }

}
