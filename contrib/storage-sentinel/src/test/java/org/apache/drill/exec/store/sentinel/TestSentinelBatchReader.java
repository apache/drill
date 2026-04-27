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

import okhttp3.mockwebserver.MockResponse;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

public class TestSentinelBatchReader extends SentinelTestBase {

  @BeforeClass
  public static void setupPlugin() throws Exception {
    String mockServerUrl = getMockServerUrl();
    String tokenEndpoint = mockServerUrl + "token";
    SentinelStoragePluginConfig config = new SentinelStoragePluginConfig(
        "workspace-id",
        null,
        "tenant-id",
        "client-id",
        "client-secret",
        "P1D",
        10000,
        new ArrayList<>(),
        AuthMode.SHARED_USER,
        null,
        mockServerUrl,
        tokenEndpoint,
        false
    );
    config.setEnabled(true);
    cluster.defineStoragePlugin("sentinel", config);
  }

  @Test
  public void testSelectAllColumns() throws Exception {
    String responseJson = "{\n" +
        "  \"tables\": [\n" +
        "    {\n" +
        "      \"name\": \"PrimaryResult\",\n" +
        "      \"columns\": [\n" +
        "        {\"name\": \"AlertName\", \"type\": \"string\"},\n" +
        "        {\"name\": \"Severity\", \"type\": \"string\"},\n" +
        "        {\"name\": \"Count\", \"type\": \"long\"}\n" +
        "      ],\n" +
        "      \"rows\": [\n" +
        "        [\"Alert1\", \"High\", 5],\n" +
        "        [\"Alert2\", \"Medium\", 3]\n" +
        "      ]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    String tokenResponse = "{\"access_token\": \"test-token\", \"expires_in\": 3600}";
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(tokenResponse));
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

    String sql = "SELECT AlertName, Severity, Count FROM sentinel.SecurityAlert";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("AlertName", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("Severity", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("Count", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("Alert1", "High", 5L)
        .addRow("Alert2", "Medium", 3L)
        .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testSelectSpecificColumns() throws Exception {
    String responseJson = "{\n" +
        "  \"tables\": [\n" +
        "    {\n" +
        "      \"columns\": [\n" +
        "        {\"name\": \"AlertName\", \"type\": \"string\"},\n" +
        "        {\"name\": \"Severity\", \"type\": \"string\"}\n" +
        "      ],\n" +
        "      \"rows\": [\n" +
        "        [\"Malware\", \"Critical\"],\n" +
        "        [\"Phishing\", \"High\"]\n" +
        "      ]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    String tokenResponse = "{\"access_token\": \"test-token\", \"expires_in\": 3600}";
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(tokenResponse));
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

    String sql = "SELECT AlertName, Severity FROM sentinel.SecurityAlert";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("AlertName", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("Severity", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("Malware", "Critical")
        .addRow("Phishing", "High")
        .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testAllDataTypes() throws Exception {
    String responseJson = "{\n" +
        "  \"tables\": [\n" +
        "    {\n" +
        "      \"columns\": [\n" +
        "        {\"name\": \"StringCol\", \"type\": \"string\"},\n" +
        "        {\"name\": \"IntCol\", \"type\": \"int\"},\n" +
        "        {\"name\": \"LongCol\", \"type\": \"long\"},\n" +
        "        {\"name\": \"RealCol\", \"type\": \"real\"},\n" +
        "        {\"name\": \"BoolCol\", \"type\": \"bool\"}\n" +
        "      ],\n" +
        "      \"rows\": [\n" +
        "        [\"test\", 42, 1000, 3.14, true]\n" +
        "      ]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    String tokenResponse = "{\"access_token\": \"test-token\", \"expires_in\": 3600}";
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(tokenResponse));
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

    String sql = "SELECT StringCol, IntCol, LongCol, RealCol, BoolCol FROM sentinel.AllTypes";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("StringCol", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("IntCol", TypeProtos.MinorType.INT, TypeProtos.DataMode.OPTIONAL)
        .add("LongCol", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("RealCol", TypeProtos.MinorType.FLOAT8, TypeProtos.DataMode.OPTIONAL)
        .add("BoolCol", TypeProtos.MinorType.BIT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("test", 42, 1000L, 3.14, true)
        .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testNullValues() throws Exception {
    String responseJson = "{\n" +
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

    String tokenResponse = "{\"access_token\": \"test-token\", \"expires_in\": 3600}";
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(tokenResponse));
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

    String sql = "SELECT Col1, Col2 FROM sentinel.NullTest";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("Col1", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("Col2", TypeProtos.MinorType.INT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(null, 123)
        .addRow("value", null)
        .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testEmptyResult() throws Exception {
    String responseJson = "{\n" +
        "  \"tables\": [\n" +
        "    {\n" +
        "      \"columns\": [\n" +
        "        {\"name\": \"AlertName\", \"type\": \"string\"},\n" +
        "        {\"name\": \"Severity\", \"type\": \"string\"}\n" +
        "      ],\n" +
        "      \"rows\": []\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    String tokenResponse = "{\"access_token\": \"test-token\", \"expires_in\": 3600}";
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(tokenResponse));
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

    String sql = "SELECT AlertName, Severity FROM sentinel.EmptyTable";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("AlertName", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("Severity", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testMultipleRows() throws Exception {
    String responseJson = "{\n" +
        "  \"tables\": [\n" +
        "    {\n" +
        "      \"columns\": [\n" +
        "        {\"name\": \"Name\", \"type\": \"string\"},\n" +
        "        {\"name\": \"Value\", \"type\": \"int\"}\n" +
        "      ],\n" +
        "      \"rows\": [\n" +
        "        [\"Alert1\", 10],\n" +
        "        [\"Alert2\", 20],\n" +
        "        [\"Alert3\", 30]\n" +
        "      ]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    String tokenResponse = "{\"access_token\": \"test-token\", \"expires_in\": 3600}";
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(tokenResponse));
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

    String sql = "SELECT Name, Value FROM sentinel.MultiRow";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("Name", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("Value", TypeProtos.MinorType.INT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("Alert1", 10)
        .addRow("Alert2", 20)
        .addRow("Alert3", 30)
        .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testLargeNumbers() throws Exception {
    String responseJson = "{\n" +
        "  \"tables\": [\n" +
        "    {\n" +
        "      \"columns\": [{\"name\": \"BigNumber\", \"type\": \"long\"}],\n" +
        "      \"rows\": [[9223372036854775807]]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    String tokenResponse = "{\"access_token\": \"test-token\", \"expires_in\": 3600}";
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(tokenResponse));
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

    String sql = "SELECT BigNumber FROM sentinel.LargeNumbers";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("BigNumber", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(9223372036854775807L)
        .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testNegativeNumbers() throws Exception {
    String responseJson = "{\n" +
        "  \"tables\": [\n" +
        "    {\n" +
        "      \"columns\": [\n" +
        "        {\"name\": \"IntVal\", \"type\": \"int\"},\n" +
        "        {\"name\": \"RealVal\", \"type\": \"real\"}\n" +
        "      ],\n" +
        "      \"rows\": [[-42, -3.14]]\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    String tokenResponse = "{\"access_token\": \"test-token\", \"expires_in\": 3600}";
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(tokenResponse));
    mockWebServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

    String sql = "SELECT IntVal, RealVal FROM sentinel.NegativeNumbers";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("IntVal", TypeProtos.MinorType.INT, TypeProtos.DataMode.OPTIONAL)
        .add("RealVal", TypeProtos.MinorType.FLOAT8, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(-42, -3.14)
        .build();

    RowSetUtilities.verify(expected, results);
  }
}
