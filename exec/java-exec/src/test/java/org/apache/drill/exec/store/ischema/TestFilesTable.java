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
package org.apache.drill.exec.store.ischema;

import org.apache.drill.categories.SqlTest;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(SqlTest.class)
public class TestFilesTable extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);

    // create one workspace named files
    File filesWorkspace = cluster.makeDataDir("files", null, null);

    // add data to the workspace: one file and folder with one file
    assertTrue(new File(filesWorkspace, "file1.txt").createNewFile());
    File folder = new File(filesWorkspace, "folder");
    assertTrue(folder.mkdir());
    assertTrue(new File(folder, "file2.txt").createNewFile());
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSelectWithoutRecursion() throws Exception {
    client.testBuilder()
        .sqlQuery("select schema_name, root_schema_name, workspace_name, file_name, relative_path, is_directory, is_file from INFORMATION_SCHEMA.`FILES`")
        .unOrdered()
        .baselineColumns("schema_name", "root_schema_name", "workspace_name", "file_name", "relative_path", "is_directory", "is_file")
        .baselineValues("dfs.files", "dfs", "files", "file1.txt", "file1.txt", false, true)
        .baselineValues("dfs.files", "dfs", "files", "folder", "folder", true, false)
        .go();
  }

  @Test
  public void testSelectWithRecursion() throws Exception {
    try {
      client.alterSession(ExecConstants.LIST_FILES_RECURSIVELY, true);
      client.testBuilder()
          .sqlQuery("select schema_name, root_schema_name, workspace_name, file_name, relative_path, is_directory, is_file from INFORMATION_SCHEMA.`FILES`")
          .unOrdered()
          .baselineColumns("schema_name", "root_schema_name", "workspace_name", "file_name", "relative_path", "is_directory", "is_file")
          .baselineValues("dfs.files", "dfs", "files", "file1.txt", "file1.txt", false, true)
          .baselineValues("dfs.files", "dfs", "files", "folder", "folder", true, false)
          .baselineValues("dfs.files", "dfs", "files", "file2.txt", "folder/file2.txt", false, true)
          .go();
    } finally {
      client.resetSession(ExecConstants.LIST_FILES_RECURSIVELY);
    }

  }

  @Test
  public void testShowFilesWithInCondition() throws Exception {
    client.testBuilder()
        .sqlQuery("show files in dfs.`files`")
        .unOrdered()
        .sqlBaselineQuery("select * from INFORMATION_SCHEMA.`FILES` where schema_name = 'dfs.files'")
        .go();
  }

  @Test
  public void testShowFilesForSpecificFolderSuccess() throws Exception {
    try {
      client.alterSession(ExecConstants.LIST_FILES_RECURSIVELY, true);
      client.testBuilder()
          .sqlQuery("show files in dfs.`files`.folder")
          .unOrdered()
          .sqlBaselineQuery("select * from INFORMATION_SCHEMA.`FILES` where schema_name = 'dfs.files' and relative_path like 'folder/%'")
          .go();
    } finally {
      client.resetSession(ExecConstants.LIST_FILES_RECURSIVELY);
    }
  }

  @Test
  public void testShowFilesForSpecificFolderFailure() throws Exception {
    thrown.expect(UserRemoteException.class);
    thrown.expectMessage(String.format("To SHOW FILES in specific directory, enable option %s", ExecConstants.LIST_FILES_RECURSIVELY));
    queryBuilder().sql("show files in dfs.`files`.folder").run();
  }

  @Test
  public void testShowFilesWithUseClause() throws Exception {
    queryBuilder().sql("use dfs.`files`").run();
    client.testBuilder()
        .sqlQuery("show files")
        .unOrdered()
        .sqlBaselineQuery("select * from INFORMATION_SCHEMA.`FILES` where schema_name = 'dfs.files'")
        .go();
  }

  @Test
  public void testShowFilesWithPartialUseClause() throws Exception {
    queryBuilder().sql("use dfs").run();
    client.testBuilder()
        .sqlQuery("show files in `files`")
        .unOrdered()
        .sqlBaselineQuery("select * from INFORMATION_SCHEMA.`FILES` where schema_name = 'dfs.files'")
        .go();
  }

  @Test
  public void testShowFilesForDefaultSchema() throws Exception {
    queryBuilder().sql("use dfs").run();
    client.testBuilder()
        .sqlQuery("show files")
        .unOrdered()
        .sqlBaselineQuery("select * from INFORMATION_SCHEMA.`FILES` where schema_name = 'dfs.default'")
        .go();
  }

  @Test
  public void testFilterPushDown_None() throws Exception {
    String plan = queryBuilder().sql("select * from INFORMATION_SCHEMA.`FILES` where file_name = 'file1.txt'").explainText();
    assertTrue(plan.contains("filter=null"));
    assertTrue(plan.contains("Filter(condition="));
  }

  @Test
  public void testFilterPushDown_Partial() throws Exception {
    String plan = queryBuilder().sql("select * from INFORMATION_SCHEMA.`FILES` where schema_name = 'dfs.files' and file_name = 'file1.txt'").explainText();
    assertTrue(plan.contains("filter=booleanand(equal(Field=SCHEMA_NAME,Literal=dfs.files))"));
    assertTrue(plan.contains("Filter(condition="));
  }

  @Test
  public void testFilterPushDown_Full() throws Exception {
    String plan = queryBuilder().sql("select * from INFORMATION_SCHEMA.`FILES` where schema_name = 'dfs.files'").explainText();
    assertTrue(plan.contains("filter=equal(Field=SCHEMA_NAME,Literal=dfs.files)"));
    assertFalse(plan.contains("Filter(condition="));
  }

}

