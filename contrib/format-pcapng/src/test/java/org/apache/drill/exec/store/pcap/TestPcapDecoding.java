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
package org.apache.drill.exec.store.pcap;

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

public class TestPcapDecoding extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("pcap/"));
  }

  @Test
  public void testDecoderColumns() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.echo_test.text as text, decode_error from dfs.`pcap/echo.pcap` t";
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
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testNotACaptureFileReturnsErrorRow() throws Exception {
    String sql = "select src_ip, decode_error from dfs.`pcap/lfs_pointer.pcap`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_ip", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow(null, "file: not a PCAP or PCAP-NG file: Bad magic number = 73726576")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testBadRecordStopsWithErrorRow() throws Exception {
    String sql = "select src_ip, decode_error from dfs.`pcap/bad_record.pcap`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    assertEquals(2, results.rowCount());
    results.clear();
    sql = "select decode_error from dfs.`pcap/bad_record.pcap` where decode_error is not null";
    String error = client.queryBuilder().sql(sql).singletonString();
    assertTrue(error, error.startsWith("file: invalid packet record after packet 1: Packet too long"));
  }

  @Test
  public void testHttpPackets() throws Exception {
    String sql = "select t.parsed_data.http.method as method, t.parsed_data.http.uri as uri, " +
        "t.parsed_data.http.host as host, t.parsed_data.http.status_code as status " +
        "from dfs.`pcap/http.pcap` t where parsed_protocol = 'http'";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("method", MinorType.VARCHAR)
        .addNullable("uri", MinorType.VARCHAR)
        .addNullable("host", MinorType.VARCHAR)
        .addNullable("status", MinorType.INT)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        // Capture order, as listed by scapy
        .addRow("GET", "/download.html", "www.ethereal.com", null)
        .addRow(null, null, null, 200)
        .addRow("GET", "/pagead/ads?client=ca-pub-2309191948673629&random=1084443430285&lmt=1082467020&format=468x60_as&output=html&url=http%3A%2F%2Fwww.ethereal.com%2Fdownload.html&color_bg=FFFFFF&color_text=333333&color_link=000000&color_url=666633&color_border=666633", "pagead2.googlesyndication.com", null)
        .addRow(null, null, null, 200)
        .addRow(null, null, null, 200)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testOversizedSnapshotLengthIsClamped() throws Exception {
    // The buffer is sized from a capped snapshot length; normal packets still read
    String sql = "select src_ip, decode_error from dfs.`pcap/huge_snaplen.pcap`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_ip", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("10.0.0.1", null)
        .addRow("10.0.0.1", null)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSessionModeReportsPacketErrors() throws Exception {
    String sql = "select decode_error from table(dfs.`pcap/malformed.pcap` (type => 'pcap', sessionizeTCPStreams => true)) " +
        "where decode_error is not null";
    assertEquals("packet: Invalid IPv4 header length 12", client.queryBuilder().sql(sql).singletonString());
  }
}
