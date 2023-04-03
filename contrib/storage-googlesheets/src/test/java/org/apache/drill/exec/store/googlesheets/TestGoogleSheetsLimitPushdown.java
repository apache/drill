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
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.common.util.JacksonUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.oauth.PersistentTokenTable;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

@Ignore("This test requires a live connection to Google Sheets.  Please run tests manually.")
public class TestGoogleSheetsLimitPushdown extends ClusterTest {

  private static final String AUTH_URI = "https://accounts.google.com/o/oauth2/auth";
  private static final String TOKEN_URI = "https://oauth2.googleapis.com/token";
  private static final List<String> REDIRECT_URI = new ArrayList<>(Arrays.asList("urn:ietf:wg:oauth:2.0:oob", "http://localhost"));

  private static StoragePluginRegistry pluginRegistry;
  private static String accessToken;
  private static String refreshToken;
  private static String sheetID;

  @BeforeClass
  public static void init() throws Exception {

    String oauthJson = Files.asCharSource(DrillFileUtils.getResourceAsFile("/tokens/oauth_tokens.json"), Charsets.UTF_8).read();

    ObjectMapper mapper = JacksonUtils.createObjectMapper();
    Map<String,String> tokenMap = mapper.readValue(oauthJson, Map.class);

    String clientID = tokenMap.get("client_id");
    String clientSecret = tokenMap.get("client_secret");
    accessToken = tokenMap.get("access_token");
    refreshToken = tokenMap.get("refresh_token");
    sheetID = tokenMap.get("sheet_id");

    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
      .configProperty(ExecConstants.HTTP_ENABLE, true)
      .configProperty(ExecConstants.HTTP_PORT_HUNT, true)
      .configProperty(ExecConstants.IMPERSONATION_ENABLED, true);

    startCluster(builder);

    int portNumber = cluster.drillbit().getWebServerPort();

    pluginRegistry = cluster.drillbit().getContext().getStorage();
    GoogleSheetsStoragePluginConfig config = GoogleSheetsStoragePluginConfig.builder()
      .clientID(clientID)
      .clientSecret(clientSecret)
      .redirectUris(REDIRECT_URI)
      .authUri(AUTH_URI)
      .tokenUri(TOKEN_URI)
      .allTextMode(false)
      .extractHeaders(true)
      .build();

    config.setEnabled(true);
    pluginRegistry.validatedPut("googlesheets", config);
  }

  @Test
  public void testLimit() throws Exception {
    try {
      initializeTokens();
    } catch (PluginException e) {
      fail(e.getMessage());
    }

    String sql = String.format("SELECT * FROM googlesheets.`%s`.`MixedSheet` LIMIT 5", sheetID);
    queryBuilder()
      .sql(sql)
      .planMatcher()
      .include("Limit", "maxRecords=5")
      .match();
  }

  @Test
  public void testLimitWithOrderBy() throws Exception {
    try {
      initializeTokens();
    } catch (PluginException e) {
      fail(e.getMessage());
    }

    // Limit should not be pushed down for this example due to the sort
    String sql = String.format("SELECT * FROM googlesheets.`%s`.`MixedSheet` ORDER BY Col2 LIMIT 4", sheetID);
    queryBuilder()
      .sql(sql)
      .planMatcher()
      .include("Limit", "maxRecords=-1")
      .match();
  }

  @Test
  public void testLimitWithOffset() throws Exception {
    try {
      initializeTokens();
    } catch (PluginException e) {
      fail(e.getMessage());
    }

    // Limit should be pushed down and include the offset
    String sql = String.format("SELECT * FROM googlesheets.`%s`.`MixedSheet` LIMIT 4 OFFSET 5", sheetID);
    queryBuilder()
      .sql(sql)
      .planMatcher()
      .include("Limit", "maxRecords=9")
      .match();
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
