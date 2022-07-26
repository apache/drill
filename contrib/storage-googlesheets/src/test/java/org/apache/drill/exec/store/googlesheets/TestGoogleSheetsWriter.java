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

package org.apache.drill.exec.store.googlesheets;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.categories.RowSetTest;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.oauth.PersistentTokenTable;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(RowSetTest.class)
@Ignore("These tests require a live Google Sheets connection.  Please run manually.")
public class TestGoogleSheetsWriter extends ClusterTest {
  private static final String AUTH_URI = "https://accounts.google.com/o/oauth2/auth";
  private static final String TOKEN_URI = "https://oauth2.googleapis.com/token";
  private static final List<String> REDIRECT_URI = new ArrayList<>(Arrays.asList("urn:ietf:wg:oauth:2.0:oob", "http://localhost"));

  private static StoragePluginRegistry pluginRegistry;
  private static String accessToken;
  private static String refreshToken;

  // Note on testing:  Testing the writing capabilites of this plugin is challenging.
  // The primary issue is that when you execute a CTAS query, you do so using the file name.
  // However, it does not seem possible to retrieve the created file's ID which is what you
  // need to actually verify that the query successfully wrote the results.  Therefore, at this
  // juncture, I can only recommend manual tests for the writing capabilities of this plugin.

  @BeforeClass
  public static void init() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get(""));

    String oauthJson = Files.asCharSource(DrillFileUtils.getResourceAsFile("/tokens/oauth_tokens.json"), Charsets.UTF_8).read();

    ObjectMapper mapper = new ObjectMapper();
    Map<String,String> tokenMap = mapper.readValue(oauthJson, Map.class);

    String clientID = tokenMap.get("client_id");
    String clientSecret = tokenMap.get("client_secret");
    accessToken = tokenMap.get("access_token");
    refreshToken = tokenMap.get("refresh_token");

    pluginRegistry = cluster.drillbit().getContext().getStorage();
    GoogleSheetsStoragePluginConfig config = GoogleSheetsStoragePluginConfig.builder()
      .clientID(clientID)
      .clientSecret(clientSecret)
      .redirectUris(REDIRECT_URI)
      .authUri(AUTH_URI)
      .tokenUri(TOKEN_URI)
      .build();

    config.setEnabled(true);
    pluginRegistry.validatedPut("googlesheets", config);
  }

  @Test
  public void testBasicCTAS() throws Exception {
    try {
      initializeTokens();
    } catch (PluginException e) {
      fail(e.getMessage());
    }

    String query = "CREATE TABLE googlesheets.`test_sheet`.`test_table` (ID, NAME) AS " +
      "SELECT * FROM (VALUES(1,2), (3,4))";
    // Create the table and insert the values
    QuerySummary insertResults = queryBuilder().sql(query).run();
    assertTrue(insertResults.succeeded());
  }

  /**
   * This function is used for testing only.  It initializes a {@link PersistentTokenTable} and populates it
   * with a valid access and refresh token.
   * @throws PluginException If anything goes wrong
   */
  private void initializeTokens() throws PluginException {
    GoogleSheetsStoragePlugin plugin = (GoogleSheetsStoragePlugin) pluginRegistry.getPlugin("googlesheets");
    plugin.initializeTokenTableForTesting();
    PersistentTokenTable tokenTable = plugin.getTokenTable();
    tokenTable.setAccessToken(accessToken);
    tokenTable.setRefreshToken(refreshToken);
    tokenTable.setExpiresIn("50000");
  }
}
