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
package org.apache.drill.exec.store.pcap.protocol.icmp;

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

/** Queries of the icmp and arp decoders through both readers. Fixtures: fixtures/icmp_arp_fixtures.py. */
public class TestIcmpArpQueries extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("decoders/"));
  }

  @Test
  public void testIcmpPcapng() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.icmp.version as version, " +
        "t.parsed_data.icmp.type_name as type_name, t.parsed_data.icmp.code_name as code_name, " +
        "t.parsed_data.icmp.identifier as identifier, t.parsed_data.icmp.mtu as mtu, " +
        "t.parsed_data.icmp.target_address as target, t.parsed_data.icmp.original_dst_ip as original_dst, " +
        "t.parsed_data.icmp.original_dst_port as original_port, decode_error " +
        "from dfs.`decoders/icmp/icmp.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("version", MinorType.INT)
        .addNullable("type_name", MinorType.VARCHAR)
        .addNullable("code_name", MinorType.VARCHAR)
        .addNullable("identifier", MinorType.INT)
        .addNullable("mtu", MinorType.INT)
        .addNullable("target", MinorType.VARCHAR)
        .addNullable("original_dst", MinorType.VARCHAR)
        .addNullable("original_port", MinorType.INT)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("icmp", 4, "echo_request", null, 0x1234, null, null, null, null, null)
        .addRow("icmp", 4, "echo_reply", null, 0x1234, null, null, null, null, null)
        .addRow("icmp", 4, "destination_unreachable", "port_unreachable", null, null, null, "8.8.8.8", 53, null)
        .addRow("icmp", 6, "neighbor_solicitation", null, null, null, "fe80:0:0:0:0:0:0:2", null, null, null)
        .addRow("icmp", 6, "packet_too_big", null, null, 1280, null, "2001:db8:0:0:0:0:0:2", 443, null)
        .addRow(null, null, null, null, null, null, null, null, null, null)
        .addRow("icmp", null, null, null, null, null, null, null, null, "icmp: truncated echo_request: 6 bytes, needs 8")
        .addRow(null, null, null, null, null, null, null, null, null, null)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testArpPcapng() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.arp.operation_name as op, " +
        "t.parsed_data.arp.sender_mac as sender_mac, t.parsed_data.arp.sender_ip as sender_ip, " +
        "t.parsed_data.arp.target_ip as target_ip, t.parsed_data.arp.is_gratuitous as gratuitous, " +
        "t.parsed_data.arp.is_probe as probe, decode_error from dfs.`decoders/arp/arp.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("op", MinorType.VARCHAR)
        .addNullable("sender_mac", MinorType.VARCHAR)
        .addNullable("sender_ip", MinorType.VARCHAR)
        .addNullable("target_ip", MinorType.VARCHAR)
        .addNullable("gratuitous", MinorType.BIT)
        .addNullable("probe", MinorType.BIT)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("arp", "request", "02:00:00:00:00:01", "10.0.0.1", "10.0.0.2", false, false, null)
        .addRow("arp", "reply", "02:00:00:00:00:02", "10.0.0.2", "10.0.0.1", false, false, null)
        .addRow("arp", "request", "02:00:00:00:00:01", "10.0.0.1", "10.0.0.1", true, false, null)
        .addRow("arp", "request", "02:00:00:00:00:01", "0.0.0.0", "10.0.0.9", false, true, null)
        .addRow(null, null, null, null, null, null, null, null)
        .addRow("arp", null, null, null, null, null, null, "arp: truncated: 20 bytes, needs 28")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testClassicPcap() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.icmp.type_name as icmp_type, " +
        "t.parsed_data.icmp.sequence as seq, t.parsed_data.arp.operation_name as arp_op, " +
        "t.parsed_data.arp.target_ip as arp_target, decode_error from dfs.`decoders/icmp/icmp_arp.pcap` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("icmp_type", MinorType.VARCHAR)
        .addNullable("seq", MinorType.INT)
        .addNullable("arp_op", MinorType.VARCHAR)
        .addNullable("arp_target", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("arp", null, null, "request", "10.0.0.2", null)
        .addRow("icmp", "echo_request", 1, null, null, null)
        .addRow("icmp", null, null, null, null, "icmp: truncated echo_request: 6 bytes, needs 8")
        .addRow(null, null, null, null, null, null)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
