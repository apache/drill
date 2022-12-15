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

package org.apache.drill.exec.store;

import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.OAuthConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.store.easy.json.JSONFormatConfig;
import org.apache.drill.exec.store.easy.text.TextFormatConfig;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


/**
 * This class tests the Box / Drill connectivity.  This class requires a Box connection, so these
 * tests must be run manually. Additionally, Box access and refresh tokens seem to be reset after
 * every API call, so it is not possible, unlike GoogleSheets, to store access tokens for testing.
 *<p>
 * Testing instructions can be found in the developer documentation in the docs folder.  TL;DR, you
 * will need a developer token.
 */
@Ignore("Please create a Box API key and run these tests manually")
public class BoxFileSystemTest extends ClusterTest {

  private static final String ACCESS_TOKEN = "<Your Box Access Token Here>";

  @BeforeClass
  public static void setup() throws Exception {
    assertFalse(ACCESS_TOKEN.equalsIgnoreCase("<Your Box Access Token Here>"));

    startCluster(ClusterFixture.builder(dirTestWatcher));

    Map<String, String> boxConfigVars = new HashMap<>();
    boxConfigVars.put("boxAccessToken", ACCESS_TOKEN);

    // Create workspaces
    WorkspaceConfig rootWorkspace = new WorkspaceConfig("/", false, null, false);
    WorkspaceConfig csvWorkspace = new WorkspaceConfig("/csv", false, null, false);
    Map<String, WorkspaceConfig> workspaces = new HashMap<>();
    workspaces.put("root", rootWorkspace);
    workspaces.put("csv", csvWorkspace);

    // Add formats
    Map<String, FormatPluginConfig> formats = new HashMap<>();
    List<String> jsonExtensions = new ArrayList<>();
    jsonExtensions.add("json");
    FormatPluginConfig jsonFormatConfig = new JSONFormatConfig(jsonExtensions, null, null, null, null, null);

    // CSV Format
    List<String> csvExtensions = new ArrayList<>();
    csvExtensions.add("csv");
    csvExtensions.add("csvh");
    FormatPluginConfig csvFormatConfig = new TextFormatConfig(csvExtensions, "\n", ",", "\"", null, null, false, true);


    Map<String, String> authParams = new HashMap<>();
    authParams.put("response_type", "code");

    OAuthConfig oAuthConfig = OAuthConfig.builder()
      .authorizationURL("https://account.box.com/api/oauth2/authorize")
      .callbackURL("http://localhost:8047/credentials/box_test/update_oauth2_authtoken")
      .authorizationParams(authParams)
      .build();


    Map<String,String> credentials = new HashMap<>();
    credentials.put("clientID", "<your client ID>");
    credentials.put("clientSecret", "<your client secret>");
    credentials.put("tokenURI", "https://api.box.com/oauth2/token");

    CredentialsProvider credentialsProvider = new PlainCredentialsProvider(credentials);

    StoragePluginConfig boxConfig = new FileSystemConfig("box:///", null, null, boxConfigVars,
      workspaces, formats, oAuthConfig, AuthMode.SHARED_USER.name(), credentialsProvider);
    boxConfig.setEnabled(true);

    cluster.defineStoragePlugin("box_test", boxConfig);
    cluster.defineFormat("box_test", "json", jsonFormatConfig);
    cluster.defineFormat("box_test", "csv", csvFormatConfig);
  }


  @Test
  public void testListFiles() throws Exception {
    String sql = "SHOW FILES IN box_test.root";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(6, results.rowCount());
    results.clear();
  }

  @Test
  public void testStarQuery() throws Exception {
    String sql = "SELECT * FROM box_test.root.`hdf-test.csv` LIMIT 10";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(10, results.rowCount());
    results.clear();
  }

  @Test
  public void testCSVQueryWithWorkspace() throws Exception {
    String sql = "select * from `box_test`.`csv`.`hdf-test.csv` LIMIT 5";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(5, results.rowCount());
    results.clear();
  }

  @Test
  public void testJSONQuery() throws Exception {
    String sql = "SELECT * FROM `box_test`.root.`http-pcap.json` LIMIT 5";

    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(5, results.rowCount());
    assertEquals(7,results.batchSchema().getFieldCount());
    results.clear();
  }
}
