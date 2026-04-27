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

package org.apache.drill.exec.store.sentinel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestSentinelBatchReader {
  private ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testParseSimpleSecurityAlertResponse() throws Exception {
    String jsonResponse = "{\n" +
        "  \"tables\": [\n" +
        "    {\n" +
        "      \"name\": \"PrimaryResult\",\n" +
        "      \"columns\": [\n" +
        "        {\"name\": \"AlertName\", \"type\": \"string\"},\n" +
        "        {\"name\": \"Severity\", \"type\": \"string\"},\n" +
        "        {\"name\": \"Count\", \"type\": \"long\"},\n" +
        "        {\"name\": \"Active\", \"type\": \"bool\"}\n" +
        "      ],\n" +
        "      \"rows\": [\n" +
        "        [\"Alert1\", \"High\", 5, true],\n" +
        "        [\"Alert2\", \"Medium\", 3, false]\n" +
        "      ]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    JsonNode root = mapper.readTree(jsonResponse);
    JsonNode tables = root.get("tables");

    assertNotNull(tables);
    assertTrue(tables.isArray());
    assertEquals(1, tables.size());

    JsonNode table = tables.get(0);
    JsonNode columns = table.get("columns");
    JsonNode rows = table.get("rows");

    assertEquals(4, columns.size());
    assertEquals(2, rows.size());

    JsonNode firstColumn = columns.get(0);
    assertEquals("AlertName", firstColumn.get("name").asText());
    assertEquals("string", firstColumn.get("type").asText());

    JsonNode firstRow = rows.get(0);
    assertEquals("Alert1", firstRow.get(0).asText());
    assertEquals("High", firstRow.get(1).asText());
    assertEquals(5, firstRow.get(2).asLong());
    assertTrue(firstRow.get(3).asBoolean());
  }

  @Test
  public void testParseTypeMappings() throws Exception {
    String jsonResponse = "{\n" +
        "  \"tables\": [\n" +
        "    {\n" +
        "      \"columns\": [\n" +
        "        {\"name\": \"StringCol\", \"type\": \"string\"},\n" +
        "        {\"name\": \"IntCol\", \"type\": \"int\"},\n" +
        "        {\"name\": \"LongCol\", \"type\": \"long\"},\n" +
        "        {\"name\": \"RealCol\", \"type\": \"real\"},\n" +
        "        {\"name\": \"BoolCol\", \"type\": \"bool\"},\n" +
        "        {\"name\": \"DatetimeCol\", \"type\": \"datetime\"}\n" +
        "      ],\n" +
        "      \"rows\": [\n" +
        "        [\"value\", 42, 1000, 3.14, true, \"2026-04-26T10:30:00Z\"]\n" +
        "      ]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    JsonNode root = mapper.readTree(jsonResponse);
    JsonNode columns = root.get("tables").get(0).get("columns");

    String[] expectedTypes = {"string", "int", "long", "real", "bool", "datetime"};
    for (int i = 0; i < expectedTypes.length; i++) {
      assertEquals(expectedTypes[i], columns.get(i).get("type").asText());
    }
  }

  @Test
  public void testParseEmptyResult() throws Exception {
    String jsonResponse = "{\n" +
        "  \"tables\": [\n" +
        "    {\n" +
        "      \"columns\": [\n" +
        "        {\"name\": \"Column1\", \"type\": \"string\"}\n" +
        "      ],\n" +
        "      \"rows\": []\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    JsonNode root = mapper.readTree(jsonResponse);
    JsonNode rows = root.get("tables").get(0).get("rows");

    assertEquals(0, rows.size());
  }

  @Test
  public void testParseNullValues() throws Exception {
    String jsonResponse = "{\n" +
        "  \"tables\": [\n" +
        "    {\n" +
        "      \"columns\": [\n" +
        "        {\"name\": \"Col1\", \"type\": \"string\"},\n" +
        "        {\"name\": \"Col2\", \"type\": \"int\"}\n" +
        "      ],\n" +
        "      \"rows\": [\n" +
        "        [null, 123],\n" +
        "        [\"value\", null]\n" +
        "      ]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    JsonNode root = mapper.readTree(jsonResponse);
    JsonNode rows = root.get("tables").get(0).get("rows");

    assertTrue(rows.get(0).get(0).isNull());
    assertTrue(rows.get(1).get(1).isNull());
  }

  @Test
  public void testParsePaginationLink() throws Exception {
    String jsonResponse = "{\n" +
        "  \"tables\": [\n" +
        "    {\n" +
        "      \"columns\": [{\"name\": \"Col1\", \"type\": \"string\"}],\n" +
        "      \"rows\": [[\"value1\"]]\n" +
        "    }\n" +
        "  ],\n" +
        "  \"@odata.nextLink\": \"https://api.loganalytics.io/v1/workspaces/abc/query?$skip=1000\"\n" +
        "}";

    JsonNode root = mapper.readTree(jsonResponse);
    JsonNode nextLink = root.get("@odata.nextLink");

    assertNotNull(nextLink);
    assertTrue(nextLink.asText().contains("skip=1000"));
  }

  @Test
  public void testParseLargeNumbers() throws Exception {
    String jsonResponse = "{\n" +
        "  \"tables\": [\n" +
        "    {\n" +
        "      \"columns\": [{\"name\": \"BigNumber\", \"type\": \"long\"}],\n" +
        "      \"rows\": [[9223372036854775807]]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    JsonNode root = mapper.readTree(jsonResponse);
    long value = root.get("tables").get(0).get("rows").get(0).get(0).asLong();

    assertEquals(9223372036854775807L, value);
  }

  @Test
  public void testParseDecimalNumbers() throws Exception {
    String jsonResponse = "{\n" +
        "  \"tables\": [\n" +
        "    {\n" +
        "      \"columns\": [\n" +
        "        {\"name\": \"RealValue\", \"type\": \"real\"},\n" +
        "        {\"name\": \"DecimalValue\", \"type\": \"decimal\"}\n" +
        "      ],\n" +
        "      \"rows\": [[1.5, 2.7]]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    JsonNode root = mapper.readTree(jsonResponse);
    JsonNode row = root.get("tables").get(0).get("rows").get(0);

    double realValue = row.get(0).asDouble();
    double decimalValue = row.get(1).asDouble();

    assertEquals(1.5, realValue, 0.01);
    assertEquals(2.7, decimalValue, 0.01);
  }
}
