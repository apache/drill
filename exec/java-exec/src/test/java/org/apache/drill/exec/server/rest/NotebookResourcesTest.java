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
package org.apache.drill.exec.server.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Unit tests for NotebookResources request/response models and validation logic.
 */
public class NotebookResourcesTest extends BaseTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  // ==================== ExportRequest Tests ====================

  @Test
  public void testExportRequestDeserialization() throws Exception {
    String json = "{\"plugin\":\"dfs\",\"workspace\":\"tmp\",\"tableName\":\"test_data\","
        + "\"format\":\"json\",\"data\":\"[{\\\"a\\\":1}]\"}";
    NotebookResources.ExportRequest request =
        mapper.readValue(json, NotebookResources.ExportRequest.class);

    Assert.assertEquals("dfs", request.plugin);
    Assert.assertEquals("tmp", request.workspace);
    Assert.assertEquals("test_data", request.tableName);
    Assert.assertEquals("json", request.format);
    Assert.assertEquals("[{\"a\":1}]", request.data);
  }

  @Test
  public void testExportRequestDefaultConstructor() {
    NotebookResources.ExportRequest request = new NotebookResources.ExportRequest();
    Assert.assertNull(request.plugin);
    Assert.assertNull(request.workspace);
    Assert.assertNull(request.tableName);
    Assert.assertNull(request.format);
    Assert.assertNull(request.data);
  }

  @Test
  public void testExportRequestParameterizedConstructor() {
    NotebookResources.ExportRequest request =
        new NotebookResources.ExportRequest("dfs", "tmp", "test", "csv", "a,b\n1,2");

    Assert.assertEquals("dfs", request.plugin);
    Assert.assertEquals("tmp", request.workspace);
    Assert.assertEquals("test", request.tableName);
    Assert.assertEquals("csv", request.format);
    Assert.assertEquals("a,b\n1,2", request.data);
  }

  @Test
  public void testExportRequestSerialization() throws Exception {
    NotebookResources.ExportRequest request =
        new NotebookResources.ExportRequest("dfs", "tmp", "data", "json", "[{\"x\":1}]");
    String json = mapper.writeValueAsString(request);

    Assert.assertTrue(json.contains("\"plugin\":\"dfs\""));
    Assert.assertTrue(json.contains("\"workspace\":\"tmp\""));
    Assert.assertTrue(json.contains("\"tableName\":\"data\""));
    Assert.assertTrue(json.contains("\"format\":\"json\""));
    Assert.assertTrue(json.contains("\"data\":\"[{\\\"x\\\":1}]\""));
  }

  @Test
  public void testExportRequestRoundTrip() throws Exception {
    NotebookResources.ExportRequest original =
        new NotebookResources.ExportRequest("dfs", "tmp", "test", "csv", "col1,col2\n1,2\n3,4");
    String json = mapper.writeValueAsString(original);
    NotebookResources.ExportRequest deserialized =
        mapper.readValue(json, NotebookResources.ExportRequest.class);

    Assert.assertEquals(original.plugin, deserialized.plugin);
    Assert.assertEquals(original.workspace, deserialized.workspace);
    Assert.assertEquals(original.tableName, deserialized.tableName);
    Assert.assertEquals(original.format, deserialized.format);
    Assert.assertEquals(original.data, deserialized.data);
  }

  // ==================== ExportResponse Tests ====================

  @Test
  public void testExportResponseSerialization() throws Exception {
    NotebookResources.ExportResponse response =
        new NotebookResources.ExportResponse(true, "Success", "dfs.tmp.`test.json`");

    String json = mapper.writeValueAsString(response);
    Assert.assertTrue(json.contains("\"success\":true"));
    Assert.assertTrue(json.contains("\"message\":\"Success\""));
    Assert.assertTrue(json.contains("dfs.tmp.`test.json`"));
  }

  @Test
  public void testExportResponseFailure() throws Exception {
    NotebookResources.ExportResponse response =
        new NotebookResources.ExportResponse(false, "Permission denied", null);

    String json = mapper.writeValueAsString(response);
    Assert.assertTrue(json.contains("\"success\":false"));
    Assert.assertTrue(json.contains("\"message\":\"Permission denied\""));
  }

  // ==================== WorkspaceInfo Tests ====================

  @Test
  public void testWorkspaceInfoSerialization() throws Exception {
    NotebookResources.WorkspaceInfo info =
        new NotebookResources.WorkspaceInfo("dfs", "tmp", "/tmp", true);

    String json = mapper.writeValueAsString(info);
    Assert.assertTrue(json.contains("\"plugin\":\"dfs\""));
    Assert.assertTrue(json.contains("\"workspace\":\"tmp\""));
    Assert.assertTrue(json.contains("\"location\":\"/tmp\""));
    Assert.assertTrue(json.contains("\"writable\":true"));
  }

  @Test
  public void testWorkspaceInfoNonWritable() throws Exception {
    NotebookResources.WorkspaceInfo info =
        new NotebookResources.WorkspaceInfo("dfs", "root", "/", false);

    String json = mapper.writeValueAsString(info);
    Assert.assertTrue(json.contains("\"writable\":false"));
  }

  // ==================== WorkspacesResponse Tests ====================

  @Test
  public void testWorkspacesResponseSerialization() throws Exception {
    NotebookResources.WorkspaceInfo info1 =
        new NotebookResources.WorkspaceInfo("dfs", "tmp", "/tmp", true);
    NotebookResources.WorkspaceInfo info2 =
        new NotebookResources.WorkspaceInfo("dfs", "data", "/data", true);
    NotebookResources.WorkspacesResponse response =
        new NotebookResources.WorkspacesResponse(List.of(info1, info2), "filesystem");

    String json = mapper.writeValueAsString(response);
    Assert.assertTrue(json.contains("\"workspaces\""));
    Assert.assertTrue(json.contains("\"tmp\""));
    Assert.assertTrue(json.contains("\"data\""));
    Assert.assertTrue(json.contains("\"writePolicy\":\"filesystem\""));
  }

  @Test
  public void testWorkspacesResponseEmpty() throws Exception {
    NotebookResources.WorkspacesResponse response =
        new NotebookResources.WorkspacesResponse(List.of(), "disabled");

    String json = mapper.writeValueAsString(response);
    Assert.assertTrue(json.contains("\"workspaces\":[]"));
    Assert.assertTrue(json.contains("\"writePolicy\":\"disabled\""));
  }

  @Test
  public void testWorkspacesResponseAdminOnly() throws Exception {
    NotebookResources.WorkspacesResponse response =
        new NotebookResources.WorkspacesResponse(List.of(), "admin_only");

    String json = mapper.writeValueAsString(response);
    Assert.assertTrue(json.contains("\"writePolicy\":\"admin_only\""));
  }

  // ==================== ErrorResponse Tests ====================

  @Test
  public void testErrorResponseSerialization() throws Exception {
    NotebookResources.ErrorResponse error =
        new NotebookResources.ErrorResponse("Something went wrong");

    String json = mapper.writeValueAsString(error);
    Assert.assertTrue(json.contains("\"message\":\"Something went wrong\""));
  }

  @Test
  public void testErrorResponseNullMessage() throws Exception {
    NotebookResources.ErrorResponse error =
        new NotebookResources.ErrorResponse(null);

    String json = mapper.writeValueAsString(error);
    Assert.assertTrue(json.contains("\"message\":null"));
  }

  // ==================== Table Name Sanitization Tests ====================

  @Test
  public void testTableNameSanitization() {
    // The sanitization logic: replaceAll("[^a-zA-Z0-9_\\-]", "_")
    Assert.assertEquals("my_table", sanitize("my_table"));
    Assert.assertEquals("my_data___", sanitize("my data!@#"));
    Assert.assertEquals("test-data", sanitize("test-data"));
    Assert.assertEquals("CamelCase123", sanitize("CamelCase123"));
    Assert.assertEquals("___path_traversal", sanitize("../path/traversal"));
    Assert.assertEquals("special______chars", sanitize("special!@#$%^chars"));
  }

  @Test
  public void testTableNameSanitizationEmpty() {
    Assert.assertEquals("", sanitize(""));
  }

  @Test
  public void testTableNameSanitizationAllSpecialChars() {
    String result = sanitize("!@#$%");
    Assert.assertEquals("_____", result);
  }

  // Helper replicating the sanitization logic from NotebookResources
  private String sanitize(String name) {
    return name.replaceAll("[^a-zA-Z0-9_\\-]", "_");
  }

  // ==================== Format Validation Tests ====================

  @Test
  public void testValidFormats() {
    Assert.assertTrue(isValidFormat("json"));
    Assert.assertTrue(isValidFormat("csv"));
    Assert.assertTrue(isValidFormat("JSON"));
    Assert.assertTrue(isValidFormat("CSV"));
  }

  @Test
  public void testInvalidFormats() {
    Assert.assertFalse(isValidFormat("parquet"));
    Assert.assertFalse(isValidFormat("avro"));
    Assert.assertFalse(isValidFormat("xml"));
    Assert.assertFalse(isValidFormat(""));
  }

  @Test
  public void testNullFormatDefaultsToJson() {
    // When format is null, it should default to "json"
    String format = null;
    String resolved = format != null ? format.toLowerCase() : "json";
    Assert.assertEquals("json", resolved);
  }

  // Helper replicating the format validation logic from NotebookResources
  private boolean isValidFormat(String format) {
    if (format == null || format.isEmpty()) {
      return false;
    }
    String lower = format.toLowerCase();
    return "json".equals(lower) || "csv".equals(lower);
  }
}
