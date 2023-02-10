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
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.oauth.PersistentTokenTable;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
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

  private static String sheetID;

  // Note on testing:  Testing the writing capabilities of this plugin is challenging.
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
    sheetID = tokenMap.get("sheet_id");

    pluginRegistry = cluster.drillbit().getContext().getStorage();
    GoogleSheetsStoragePluginConfig config = GoogleSheetsStoragePluginConfig.builder()
      .clientID(clientID)
      .clientSecret(clientSecret)
      .redirectUris(REDIRECT_URI)
      .authUri(AUTH_URI)
      .tokenUri(TOKEN_URI)
      .extractHeaders(true)
      .allTextMode(false)
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

  @Test
  public void testCTASLifecycle() throws Exception {
    // This test goes through the entire CTAS, INSERT, DROP lifecycle.
    try {
      initializeTokens();
    } catch (PluginException e) {
      fail(e.getMessage());
    }

    // We are creating a new tab in an existing GS document
    String sql = String.format("CREATE TABLE googlesheets.`%s`.`Sheet3` AS SELECT * FROM cp.`data/insert_data.csvh`", sheetID);
    QuerySummary results = queryBuilder().sql(sql).run();
    assertTrue(results.succeeded());

    // Verify the sheet was created
    sql = String.format("SELECT * FROM googlesheets.`%s`.`Sheet3`", sheetID);
    results = queryBuilder().sql(sql).run();
    assertTrue(results.succeeded());
    assertEquals(2, results.recordCount());

    // Now Insert additional records into the sheet
    sql = String.format("INSERT INTO googlesheets.`%s`.`Sheet3` SELECT * FROM cp.`data/insert_data2.csvh`", sheetID);
    results = queryBuilder().sql(sql).run();
    assertTrue(results.succeeded());

    // Verify that the records were inserted
    sql = String.format("SELECT * FROM googlesheets.`%s`.`Sheet3`", sheetID);
    RowSet rowSet = queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .addNullable("col1", MinorType.FLOAT8)
        .addNullable("col2", MinorType.FLOAT8)
        .addNullable("col3", MinorType.FLOAT8)
        .buildSchema();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow(1,2,3)
        .addRow(4,5,6)
        .addRow(7,8,9)
        .addRow(10,11,12)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(rowSet);

    // Drop the table
    sql = String.format("DROP TABLE googlesheets.`%s`.`Sheet3`", sheetID);
    results = queryBuilder().sql(sql).run();
    assertTrue(results.succeeded());

    // Verify that it's gone
    sql = String.format("SELECT * FROM googlesheets.`%s`.`Sheet3`", sheetID);
    try {
      results = queryBuilder().sql(sql).run();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("'Sheet3' not found"));
    }
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
