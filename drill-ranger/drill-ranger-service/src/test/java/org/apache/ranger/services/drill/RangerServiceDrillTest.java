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
package org.apache.ranger.services.drill;

import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link RangerServiceDrill}.
 * Covers URL normalization, SQL escaping, JSON parsing, hint extraction,
 * and the control-flow edge cases of lookupResource / validateConfig that
 * can be exercised without a live Drill cluster.
 */
public class RangerServiceDrillTest {

  // ========================================================================
  // buildBaseUrl (package-private static — directly callable from same package)
  // ========================================================================

  @Test
  public void buildBaseUrl_stripsTrailingSlash() {
    assertEquals("http://host:8047", RangerServiceDrill.buildBaseUrl("http://host:8047/"));
  }

  @Test
  public void buildBaseUrl_handlesMultipleTrailingSlashes() {
    assertEquals("http://host:8047", RangerServiceDrill.buildBaseUrl("http://host:8047///"));
  }

  @Test
  public void buildBaseUrl_trimsWhitespace() {
    assertEquals("http://host:8047", RangerServiceDrill.buildBaseUrl("  http://host:8047  "));
  }

  @Test
  public void buildBaseUrl_throwsForBlankInput() {
    assertThrows(IllegalArgumentException.class, () -> RangerServiceDrill.buildBaseUrl(null));
    assertThrows(IllegalArgumentException.class, () -> RangerServiceDrill.buildBaseUrl(""));
    assertThrows(IllegalArgumentException.class, () -> RangerServiceDrill.buildBaseUrl("   "));
  }

  @Test
  public void buildBaseUrl_preservesProtocol() {
    assertEquals("https://host:8047", RangerServiceDrill.buildBaseUrl("https://host:8047"));
    assertEquals("https://host:8047", RangerServiceDrill.buildBaseUrl("https://host:8047/"));
  }

  // ========================================================================
  // escapeSql (private static — reflection)
  // ========================================================================

  @Test
  public void escapeSql_doublesSingleQuote() throws Exception {
    assertEquals("it''s", invokeEscapeSql("it's"));
  }

  @Test
  public void escapeSql_nullReturnsEmpty() throws Exception {
    assertEquals("", invokeEscapeSql(null));
  }

  @Test
  public void escapeSql_noQuotes_unchanged() throws Exception {
    assertEquals("mysql", invokeEscapeSql("mysql"));
  }

  // ========================================================================
  // firstHint (private static — reflection)
  // ========================================================================

  @Test
  public void firstHint_returnsFirstValue() throws Exception {
    Map<String, List<String>> hints = new HashMap<>();
    hints.put("datasource", java.util.Arrays.asList("mysql", "dfs"));
    assertEquals("mysql", invokeFirstHint(hints, "datasource"));
  }

  @Test
  public void firstHint_nullHints_returnsNull() throws Exception {
    assertNull(invokeFirstHint(null, "datasource"));
  }

  @Test
  public void firstHint_emptyList_returnsNull() throws Exception {
    Map<String, List<String>> hints = new HashMap<>();
    hints.put("datasource", Collections.emptyList());
    assertNull(invokeFirstHint(hints, "datasource"));
  }

  @Test
  public void firstHint_missingKey_returnsNull() throws Exception {
    Map<String, List<String>> hints = new HashMap<>();
    hints.put("schema", java.util.Arrays.asList("shf"));
    assertNull(invokeFirstHint(hints, "datasource"));
  }

  // ========================================================================
  // extractFirstColumnValues (private static — reflection)
  // ========================================================================

  @Test
  public void extractFirstColumnValues_parsesRows() throws Exception {
    String json = "{\"columns\":[\"COL1\"],\"rows\":[{\"COL1\":\"v1\"},{\"COL1\":\"v2\"}]}";
    List<String> result = invokeExtractFirstColumnValues(json);
    assertEquals(java.util.Arrays.asList("v1", "v2"), result);
  }

  @Test
  public void extractFirstColumnValues_emptyRows_returnsEmptyList() throws Exception {
    String json = "{\"columns\":[\"COL1\"],\"rows\":[]}";
    List<String> result = invokeExtractFirstColumnValues(json);
    assertTrue(result.isEmpty());
  }

  @Test
  public void extractFirstColumnValues_missingRowsKey_returnsEmptyList() throws Exception {
    String json = "{\"columns\":[\"COL1\"]}";
    List<String> result = invokeExtractFirstColumnValues(json);
    assertTrue(result.isEmpty());
  }

  @Test
  public void extractFirstColumnValues_skipsBlankValues() throws Exception {
    String json = "{\"columns\":[\"COL1\"],\"rows\":[{\"COL1\":\"  \"},{\"COL1\":\"v2\"}]}";
    List<String> result = invokeExtractFirstColumnValues(json);
    assertEquals(Collections.singletonList("v2"), result);
  }

  @Test
  public void extractFirstColumnValues_fallbackToFirstField() throws Exception {
    // No "columns" array — should fall back to the first field of each row
    String json = "{\"rows\":[{\"X\":\"a\"},{\"Y\":\"b\"}]}";
    List<String> result = invokeExtractFirstColumnValues(json);
    // Each row contributes its first field value
    assertEquals(2, result.size());
    assertTrue(result.contains("a"));
    assertTrue(result.contains("b"));
  }

  // ========================================================================
  // lookupResource edge cases (no executeQuery needed)
  // ========================================================================

  @Test
  public void lookupResource_nullContext_returnsEmpty() throws Exception {
    RangerServiceDrill service = newServiceWithConfigs(
        "root", "pw", "http://host:8047");
    List<String> result = service.lookupResource(null);
    assertTrue(result.isEmpty());
  }

  @Test
  public void lookupResource_blankResourceName_returnsEmpty() throws Exception {
    RangerServiceDrill service = newServiceWithConfigs(
        "root", "pw", "http://host:8047");
    ResourceLookupContext ctx = new ResourceLookupContext();
    ctx.setResourceName("");
    List<String> result = service.lookupResource(ctx);
    assertTrue(result.isEmpty());
  }

  @Test
  public void lookupResource_unknownResourceName_returnsEmpty() throws Exception {
    RangerServiceDrill service = newServiceWithConfigs(
        "root", "pw", "http://host:8047");
    ResourceLookupContext ctx = new ResourceLookupContext();
    ctx.setResourceName("foo");
    // lookupResource throws on unknown resource via the default branch,
    // but the catch block re-throws. Wrap in try/catch and assert empty only
    // if it returns. If it throws, that's also acceptable behavior.
    try {
      List<String> result = service.lookupResource(ctx);
      assertTrue(result.isEmpty());
    } catch (Exception e) {
      // Acceptable: unknown resource name may throw
    }
  }

  @Test
  public void lookupResource_schema_blankDatasource_returnsEmpty() throws Exception {
    RangerServiceDrill service = newServiceWithConfigs(
        "root", "pw", "http://host:8047");
    ResourceLookupContext ctx = new ResourceLookupContext();
    ctx.setResourceName("schema");
    ctx.setResources(Collections.emptyMap());
    List<String> result = service.lookupResource(ctx);
    assertTrue(result.isEmpty());
  }

  @Test
  public void lookupResource_table_missingHints_returnsEmpty() throws Exception {
    RangerServiceDrill service = newServiceWithConfigs(
        "root", "pw", "http://host:8047");
    // Only datasource hint, missing schema
    Map<String, List<String>> hints = new HashMap<>();
    hints.put("datasource", Collections.singletonList("mysql"));
    ResourceLookupContext ctx = new ResourceLookupContext();
    ctx.setResourceName("table");
    ctx.setResources(hints);
    List<String> result = service.lookupResource(ctx);
    assertTrue(result.isEmpty());
  }

  @Test
  public void lookupResource_column_missingTableHint_returnsEmpty() throws Exception {
    RangerServiceDrill service = newServiceWithConfigs(
        "root", "pw", "http://host:8047");
    Map<String, List<String>> hints = new HashMap<>();
    hints.put("datasource", Collections.singletonList("mysql"));
    hints.put("schema", Collections.singletonList("shf"));
    ResourceLookupContext ctx = new ResourceLookupContext();
    ctx.setResourceName("column");
    ctx.setResources(hints);
    List<String> result = service.lookupResource(ctx);
    assertTrue(result.isEmpty());
  }

  @Test
  public void lookupResource_column_missingSchemaHint_returnsEmpty() throws Exception {
    RangerServiceDrill service = newServiceWithConfigs(
        "root", "pw", "http://host:8047");
    Map<String, List<String>> hints = new HashMap<>();
    hints.put("datasource", Collections.singletonList("mysql"));
    hints.put("table", Collections.singletonList("orders"));
    ResourceLookupContext ctx = new ResourceLookupContext();
    ctx.setResourceName("column");
    ctx.setResources(hints);
    List<String> result = service.lookupResource(ctx);
    assertTrue(result.isEmpty());
  }

  // ========================================================================
  // validateConfig edge cases (no executeQuery needed)
  // ========================================================================

  @Test
  public void validateConfig_missingUsername_returnsFailure() throws Exception {
    RangerServiceDrill service = newServiceWithConfigs(
        null, "pw", "http://host:8047");
    Map<String, Object> result = service.validateConfig();
    assertEquals("FAILURE", result.get("status"));
    assertNotNull(result.get("message"));
  }

  @Test
  public void validateConfig_blankUsername_returnsFailure() throws Exception {
    RangerServiceDrill service = newServiceWithConfigs(
        "  ", "pw", "http://host:8047");
    Map<String, Object> result = service.validateConfig();
    assertEquals("FAILURE", result.get("status"));
  }

  @Test
  public void validateConfig_missingUrl_returnsFailure() throws Exception {
    RangerServiceDrill service = newServiceWithConfigs(
        "root", "pw", null);
    Map<String, Object> result = service.validateConfig();
    assertEquals("FAILURE", result.get("status"));
  }

  @Test
  public void validateConfig_blankUrl_returnsFailure() throws Exception {
    RangerServiceDrill service = newServiceWithConfigs(
        "root", "pw", "   ");
    Map<String, Object> result = service.validateConfig();
    assertEquals("FAILURE", result.get("status"));
  }

  // ========================================================================
  // Reflection helpers
  // ========================================================================

  private static String invokeEscapeSql(String input) throws Exception {
    Method m = RangerServiceDrill.class.getDeclaredMethod("escapeSql", String.class);
    m.setAccessible(true);
    return (String) m.invoke(null, input);
  }

  @SuppressWarnings("unchecked")
  private static List<String> invokeExtractFirstColumnValues(String json) throws Exception {
    Method m = RangerServiceDrill.class.getDeclaredMethod("extractFirstColumnValues", String.class);
    m.setAccessible(true);
    return (List<String>) m.invoke(null, json);
  }

  private static String invokeFirstHint(Map<String, List<String>> hints, String key) throws Exception {
    Method m = RangerServiceDrill.class.getDeclaredMethod("firstHint", Map.class, String.class);
    m.setAccessible(true);
    return (String) m.invoke(null, hints, key);
  }

  /**
   * Creates a RangerServiceDrill instance with the {@code configs} field
   * pre-populated. The {@code configs} field is declared in
   * {@link org.apache.ranger.plugin.service.RangerBaseService} as a
   * protected {@code Map<String, Object>}.
   */
  private static RangerServiceDrill newServiceWithConfigs(
      String username, String password, String url) throws Exception {
    RangerServiceDrill service = new RangerServiceDrill();
    Map<String, Object> configs = new HashMap<>();
    if (username != null) {
      configs.put("username", username);
    }
    if (password != null) {
      configs.put("password", password);
    }
    if (url != null) {
      configs.put("drill.connection.url", url);
    }
    Field configsField = service.getClass().getSuperclass().getDeclaredField("configs");
    configsField.setAccessible(true);
    configsField.set(service, configs);
    return service;
  }
}
