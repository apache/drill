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
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.util.DrillFileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.oauth.PersistentTokenTable;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.io.Files;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This class tests the Google Sheets plugin. Since GoogleSheets is essentially an API, these tests
 * must be run with a live internet connection.  These tests use test data which can be found in the
 * resources directory.
 */
@Ignore("Requires live connection to GoogleSheets.  Please run tests manually.")
public class TestGoogleSheetsQueries extends ClusterTest {

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

    ObjectMapper mapper = new ObjectMapper();
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
  public void testWithSheetName() throws Exception {
    try {
      initializeTokens();
    } catch (PluginException e) {
      fail(e.getMessage());
    }

    String sql = String.format("SELECT * FROM googlesheets.`%s`.`MixedSheet` WHERE `Col2` < 6.0", sheetID);
    RowSet results = queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("Col1", MinorType.VARCHAR)
      .addNullable("Col2", MinorType.FLOAT8)
      .addNullable("Col3", MinorType.DATE)
      .buildSchema();

   RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow("Rosaline  Thales", 1.0, null)
      .addRow("Abdolhossein  Detlev", 2.0001, LocalDate.parse("2020-04-30"))
      .addRow(null, 4.0, LocalDate.parse("2020-06-30"))
      .addRow("Yunus  Elena", 3.5, LocalDate.parse("2021-01-15"))
      .addRow("Swaran  Ohiyesa", -63.8, LocalDate.parse("2021-04-08"))
      .addRow("Kalani  Godabert", 0.0, LocalDate.parse("2021-06-28"))
      .addRow("Caishen  Origenes", 5.0E-7, LocalDate.parse("2021-07-09"))
      .addRow("Toufik  Gurgen", 2.0, LocalDate.parse("2021-11-05"))
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testWithExplicitColumns() throws Exception {
    try {
      initializeTokens();
    } catch (PluginException e) {
      fail(e.getMessage());
    }

    String sql = String.format("SELECT Col1, Col3 FROM googlesheets.`%s`.`MixedSheet` WHERE `Col2` < 6.0", sheetID);
    RowSet results = queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("Col1", MinorType.VARCHAR)
      .addNullable("Col3", MinorType.DATE)
      .buildSchema();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow("Rosaline  Thales", null)
      .addRow("Abdolhossein  Detlev", LocalDate.parse("2020-04-30"))
      .addRow(null, LocalDate.parse("2020-06-30"))
      .addRow("Yunus  Elena", LocalDate.parse("2021-01-15"))
      .addRow("Swaran  Ohiyesa", LocalDate.parse("2021-04-08"))
      .addRow("Kalani  Godabert",LocalDate.parse("2021-06-28"))
      .addRow("Caishen  Origenes", LocalDate.parse("2021-07-09"))
      .addRow("Toufik  Gurgen", LocalDate.parse("2021-11-05"))
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testAggregateQuery() throws Exception {
    try {
      initializeTokens();
    } catch (PluginException e) {
      fail(e.getMessage());
    }

    String sql = String.format("SELECT EXTRACT(YEAR FROM Col3) AS event_year, COUNT(*) AS event_count FROM googlesheets.`%s`.`MixedSheet` GROUP BY event_year", sheetID);
    List<QueryDataBatch> results = queryBuilder().sql(sql).results();

    for(QueryDataBatch b : results){
      b.release();
    }
    assertEquals(4, results.size());
  }

  @Test
  public void testSerDe() throws Exception {
    try {
      initializeTokens();
    } catch (PluginException e) {
      fail(e.getMessage());
    }

    String sql = String.format("SELECT COUNT(*) FROM googlesheets.`%s`.`MixedSheet`", sheetID);
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();
    assertEquals("Counts should match", 25L, cnt);
  }

  @Test
  public void testSchemaProvisioning() throws Exception {
    try {
      initializeTokens();
    } catch (PluginException e) {
      fail(e.getMessage());
    }

    String sql = String.format("SELECT * FROM table(`googlesheets`.`%s`.`MixedSheet` (schema => 'inline=(`Col1` VARCHAR, `Col2` INTEGER, `Col3` VARCHAR)')) LIMIT 5", sheetID);
    RowSet results = queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
      .addNullable("Col1", MinorType.VARCHAR)
      .addNullable("Col2", MinorType.INT)
      .addNullable("Col3", MinorType.VARCHAR)
      .buildSchema();

    RowSet expected = client.rowSetBuilder(expectedSchema)
      .addRow("Rosaline  Thales", 1, null)
      .addRow("Abdolhossein  Detlev", 2, "2020-04-30")
      .addRow("Yosuke  Simon", null, "2020-05-22")
      .addRow(null, 4, "2020-06-30")
      .addRow("Avitus  Stribog", 500000, "2020-07-27")
      .build();

    new RowSetComparison(expected).verifyAndClearAll(results);
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
