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
package org.apache.drill.exec.store.pcapng;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Paths;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPcapngDecoding extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("pcapng/"));
  }

  @Test
  public void testDecoderColumns() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.echo_test.text as text, decode_error " +
        "from dfs.`pcapng/echo.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("text", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("echo_test", "hello", null)
        .addRow(null, null, null)
        .addRow("echo_test", null, "echo_test: bad echo")
        .addRow("echo_test", null, "echo_test: write failed: cannot write")
        .addRow("echo_test", "a long echo text", "echo_test: long text")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testPacketErrorsDoNotStopTheFile() throws Exception {
    String sql = "select interface_id, src_ip, decode_error from dfs.`pcapng/errors.pcapng`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("interface_id", MinorType.INT)
        .addNullable("src_ip", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow(0, "10.0.0.1", "packet: Invalid IPv4 header length 12")
        .addRow(0, "10.0.0.1", null)
        .addRow(5, null, "file: packet references undefined interface 5")
        .addRow(1, "10.0.0.1", "file: interface 1 has unsupported if_tsresol 127; timestamps assume microseconds")
        .addRow(null, null, "file: block at byte 348 has invalid captured length 9999")
        .addRow(0, "10.0.0.1", null)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testStructuralDamageStopsWithErrorRow() throws Exception {
    String sql = "select src_ip, decode_error from dfs.`pcapng/bad_block_length.pcapng`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_ip", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("10.0.0.1", null)
        .addRow("10.0.0.1", null)
        .addRow(null, "file: invalid block length 7 at byte 184")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testDecodingSkippedWhenNotProjected() throws Exception {
    // Without parsed_* projected, the parse failure is not reported
    String sql = "select decode_error from dfs.`pcapng/echo.pcapng` where decode_error is not null";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(0, results.rowCount());
    results.clear();
  }

  @Test
  public void testDns() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.dns.questions[0].name as qname, " +
        "t.parsed_data.dns.answers[0].data as answer, decode_error from dfs.`pcapng/dns.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("qname", MinorType.VARCHAR)
        .addNullable("answer", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("dns", "example.com", null, null)
        .addRow("dns", "example.com", "93.184.216.34", null)
        .addRow(null, null, null, null)
        .addRow("dns", null, null, "dns: truncated answer 1: needs 4 bytes at offset 41")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testOversizedBlockLengthIsReported() throws Exception {
    String sql = "select src_ip, decode_error from dfs.`pcapng/huge_block.pcapng`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_ip", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("10.0.0.1", null)
        .addRow(null, "file: invalid block length 2147483632 at byte 116")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSessionModeReportsPacketErrors() throws Exception {
    // Packets are not rows in session mode, so each distinct problem gets one error row
    String sql = "select decode_error from table(dfs.`pcapng/errors.pcapng` (type => 'pcapng', sessionizeTCPStreams => true)) " +
        "where decode_error is not null";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("packet: Invalid IPv4 header length 12")
        .addRow("file: packet references undefined interface 5")
        .addRow("file: interface 1 has unsupported if_tsresol 127; timestamps assume microseconds")
        .addRow("file: block at byte 348 has invalid captured length 9999")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testReadFailureMidFileIsReported() throws Exception {
    // A corrupted gzip file fails while decompressing, partway through the capture
    String file = "dfs.`pcapng/sniff_corrupt.pcapng.gz`";
    long packets = client.queryBuilder().sql("select count(*) from " + file + " where decode_error is null").singletonLong();
    String error = client.queryBuilder().sql("select decode_error from " + file + " where decode_error is not null").singletonString();
    assertTrue(packets > 0);
    assertTrue(error, error.startsWith("file: read failed at byte "));
  }
}
