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
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pmml4s.model.Model;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestMLUtils extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("models/"));
  }

  /** The query user handed to getModel; it is the identity reads happen as under impersonation. */
  private static final String USER = "testuser";

  private static DrillbitContext context() {
    return cluster.drillbit().getContext();
  }

  private static Model model(String location) {
    return MLUtils.getModel(location, context(), USER);
  }

  private static String expectFailure(String location) {
    try {
      MLUtils.getModel(location, context(), USER);
    } catch (UserException e) {
      return e.getMessage();
    }
    fail("Expected a UserException for " + location);
    return null;
  }

  @Test
  public void loadsAModelFromAFileSystemPlugin() {
    Model model = model("dfs.root.`models/iris_rf.pmml`");
    assertNotNull(model);
    assertEquals(3, model.inputFields().length);
  }

  @Test
  public void theFixtureScoresDifferentRowsToDifferentClasses() {
    Model model = model("dfs.root.`models/iris_rf.pmml`");
    assertEquals("setosa", predictClass(model, 5.1, 1.4, 0.2));
    assertEquals("versicolor", predictClass(model, 6.0, 4.0, 1.3));
    assertEquals("virginica", predictClass(model, 6.5, 5.5, 2.0));
  }

  private static String predictClass(Model model, double sepalLength, double petalLength, double petalWidth) {
    Map<String, Object> row = new HashMap<>();
    row.put("sepal_length", sepalLength);
    row.put("petal_length", petalLength);
    row.put("petal_width", petalWidth);
    Map<String, Object> result = model.predict(row);
    assertTrue(result.containsKey("probability_setosa"));
    return (String) result.get("predicted_species");
  }

  @Test
  public void loadsAModelFromAPluginWithoutWorkspaces() {
    Model model = model("cp.`models/iris_rf.pmml`");
    assertNotNull(model);
  }

  @Test
  public void missingModelFileFailsWithThePathInTheMessage() {
    String message = expectFailure("dfs.root.`models/no_such_model.pmml`");
    assertTrue(message, message.contains("no_such_model.pmml"));
  }

  @Test
  public void aNonFileSystemPluginIsRejected() {
    String message = expectFailure("sys.`models/iris_rf.pmml`");
    assertTrue(message, message.contains("file system storage plugin"));
  }

  @Test
  public void anUnknownPluginIsRejected() {
    String message = expectFailure("nosuchplugin.root.`models/iris_rf.pmml`");
    assertTrue(message, message.contains("was not found"));
  }

  @Test
  public void aCorruptModelFileFailsWithTheModelPath() {
    String message = expectFailure("dfs.root.`models/corrupt.pmml`");
    assertTrue(message, message.contains("corrupt.pmml"));
  }

  @Test
  public void aBacktickedUrlIsRejected() {
    String message = expectFailure("cp.`http://localhost:1/model.pmml`");
    assertTrue(message, message.contains("cannot be loaded from URLs"));
  }

  @Test
  public void traversalOutOfTheWorkspaceIsRejected() {
    String message = expectFailure("dfs.tmp.`../../etc/passwd`");
    assertTrue(message, message.contains("inside its workspace"));
  }

  @Test
  public void anAbsolutePathOutOfTheWorkspaceIsRejected() {
    String message = expectFailure("dfs.tmp.`/etc/passwd`");
    assertTrue(message, message.contains("inside its workspace"));
  }

  @Test
  public void anAbsolutePathOnAWorkspaceLessPluginIsRejected() {
    String message = expectFailure("cp.`/etc/passwd`");
    assertTrue(message, message.contains("inside its workspace"));
    message = expectFailure("cp.`../../etc/passwd`");
    assertTrue(message, message.contains("inside its workspace"));
  }

  private static FileSystemConfig configWith(Map<String, WorkspaceConfig> workspaces) {
    return new FileSystemConfig("file:///", null, workspaces, null, null);
  }

  private static WorkspaceConfig ws(String location) {
    return new WorkspaceConfig(location, true, null, false);
  }

  @Test
  public void pathJoinHandlesPrefixShapes() {
    Map<String, WorkspaceConfig> workspaces = new HashMap<>();
    workspaces.put("noslash", ws("/data/models"));
    workspaces.put("slash", ws("/data/models/"));
    workspaces.put("root", ws("/"));
    workspaces.put("empty", ws(""));
    workspaces.put("default", ws("/data/default"));
    FileSystemConfig config = configWith(workspaces);

    assertEquals("/data/models/m.pmml", MLUtils.getFullFilePath("dfs.noslash.`m.pmml`", config));
    assertEquals("/data/models/m.pmml", MLUtils.getFullFilePath("dfs.slash.`m.pmml`", config));
    assertEquals("/m.pmml", MLUtils.getFullFilePath("dfs.root.`m.pmml`", config));
    assertEquals("m.pmml", MLUtils.getFullFilePath("dfs.empty.`m.pmml`", config));
    assertEquals("/data/default/sub/m.pmml", MLUtils.getFullFilePath("dfs.`sub/m.pmml`", config));
  }

  @Test
  public void aRootWorkspaceRefusesNamesThatDoNotLandUnderIt() {
    Map<String, WorkspaceConfig> workspaces = new HashMap<>();
    workspaces.put("root", ws("/"));
    FileSystemConfig config = configWith(workspaces);
    assertEquals("/a/b.pmml", MLUtils.getFullFilePath("dfs.root.`a/b.pmml`", config));
    for (String name : new String[] {".", "..", "../etc/passwd", "a/../..", "a/../../etc/passwd"}) {
      try {
        MLUtils.getFullFilePath("dfs.root.`" + name + "`", config);
        fail("a name that does not land under the workspace must be refused: " + name);
      } catch (UserException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("inside its workspace"));
      }
    }
  }


  @Test
  public void aModelDeclaringADoctypeIsRefused() {
    String message = expectFailure("dfs.root.`models/xxe.pmml`");
    assertTrue(message, message.contains("DOCTYPE"));
    assertTrue(message, message.contains("xxe.pmml"));
  }

  @Test
  public void anExternalEntityCannotReadAFileOutsideTheWorkspace() {
    // The fixture's entity points at file:///etc/passwd. The guard must refuse the model
    // before any parser resolves it, so no part of that file can appear anywhere.
    String message = expectFailure("dfs.root.`models/xxe.pmml`");
    assertFalse(message, message.contains("root:"));
    assertFalse(message, message.contains("/bin/"));
  }

  @Test
  public void anExternalDtdSubsetIsRefused() {
    String message = expectFailure("dfs.root.`models/external_dtd.pmml`");
    assertTrue(message, message.contains("DOCTYPE"));
    assertTrue(message, message.contains("external_dtd.pmml"));
  }

  @Test
  public void theQueryUserReadsTheModelOnlyWhenImpersonationIsOn() {
    assertEquals("alice", MLUtils.fileSystemUser(true, "alice"));
    assertNull(MLUtils.fileSystemUser(false, "alice"));
    assertNull(MLUtils.fileSystemUser(true, null));
    assertNull(MLUtils.fileSystemUser(true, "   "));
    // The test cluster runs without impersonation, so reads stay on the process user.
    assertNull(MLUtils.fileSystemUser(context(), "alice"));
  }

  @Test
  public void anImpersonatedFileSystemReadsTheModel() throws Exception {
    FileSystemPlugin dfs = (FileSystemPlugin) context().getStorage().getPlugin("dfs");
    String path = MLUtils.getFullFilePath("dfs.root.`models/iris_rf.pmml`",
        (FileSystemConfig) dfs.getConfig());
    try (DrillFileSystem fs = MLUtils.openFileSystem(dfs.getFsConf(), "alice")) {
      assertTrue(path, fs.exists(new Path(path)));
    }
  }

  @Test
  public void aUriFormWorkspaceLocationResolvesInsideItself() {
    Map<String, WorkspaceConfig> workspaces = new HashMap<>();
    workspaces.put("uri", ws("file:///data/tmp"));
    FileSystemConfig config = configWith(workspaces);
    assertTrue(MLUtils.getFullFilePath("dfs.uri.`m.pmml`", config).endsWith("/data/tmp/m.pmml"));
  }

  @Test
  public void anHttpUrlIsRejected() {
    String message = expectFailure("http://localhost:1/model.pmml");
    assertTrue(message, message.contains("cannot be loaded from URLs"));
    message = expectFailure("https://example.com/model.pmml");
    assertTrue(message, message.contains("cannot be loaded from URLs"));
  }
}
