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
import static org.junit.Assert.assertNotNull;

import java.nio.file.Paths;
import java.time.Instant;

import org.apache.drill.categories.RowSetTest;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.QueryTestUtil;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(RowSetTest.class)
public class TestPcapngRecordReader extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("pcapng/"));
  }

  @Test
  public void testStarQuery() throws Exception {
    String sql = "select * from dfs.`pcapng/sniff.pcapng`";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    assertEquals(123, sets.rowCount());
    sets.clear();
  }

  @Test
  public void testExplicitQuery() throws Exception {
    String sql = "select type, packet_length, `timestamp` from dfs.`pcapng/sniff.pcapng` where type = 'ARP' limit 2";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    TupleMetadata schema = new SchemaBuilder()
        .addNullable("type", MinorType.VARCHAR)
        .addNullable("packet_length", MinorType.INT)
        .addNullable("timestamp", MinorType.TIMESTAMP)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("ARP", 60, Instant.ofEpochMilli(1518010666140L))
        .addRow("ARP", 60, Instant.ofEpochMilli(1518010666140L))
        .build();

    assertEquals(2, sets.rowCount());
    new RowSetComparison(expected).verifyAndClearAll(sets);
  }

  @Test
  public void testLimitPushdown() throws Exception {
    String sql = "select * from dfs.`pcapng/sniff.pcapng` where type = 'UDP' limit 10 offset 65";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    assertEquals(6, sets.rowCount());
    sets.clear();
  }

  @Test
  public void testSerDe() throws Exception {
    String sql = "select count(*) from dfs.`pcapng/example.pcapng`";
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();

    assertEquals("Counts should match", 1, cnt);
  }

  @Test
  public void testMixedPcapAndPcapngDirectory() throws Exception {
    // Each file must be read with the reader for its own format
    dirTestWatcher.copyResourceToRoot(Paths.get("pcapng/sniff.pcapng"), Paths.get("mixed/sniff.pcapng"));
    dirTestWatcher.copyResourceToRoot(Paths.get("pcap/tcp-1.pcap"), Paths.get("mixed/tcp-1.pcap"));
    long pcapng = queryBuilder().sql("select count(*) from dfs.`mixed/sniff.pcapng`").singletonLong();
    long pcap = queryBuilder().sql("select count(*) from dfs.`mixed/tcp-1.pcap`").singletonLong();
    long both = queryBuilder().sql("select count(*) from dfs.`mixed`").singletonLong();

    assertEquals(123, pcapng);
    assertEquals(pcapng + pcap, both);
  }

  @Test
  public void testExplicitQueryWithCompressedFile() throws Exception {
    QueryTestUtil.generateCompressedFile("pcapng/sniff.pcapng", "zip", "pcapng/sniff.pcapng.zip");
    String sql = "select type, packet_length, `timestamp` from dfs.`pcapng/sniff.pcapng.zip` where type = 'ARP' limit 2";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    TupleMetadata schema = new SchemaBuilder()
        .addNullable("type", MinorType.VARCHAR)
        .addNullable("packet_length", MinorType.INT)
        .addNullable("timestamp", MinorType.TIMESTAMP)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("ARP", 60, Instant.ofEpochMilli(1518010666140L))
        .addRow("ARP", 60, Instant.ofEpochMilli(1518010666140L))
        .build();

    assertEquals(2, sets.rowCount());
    new RowSetComparison(expected).verifyAndClearAll(sets);
  }

  @Test
  public void testCaseInsensitiveQuery() throws Exception {
    String sql = "select `timestamp`, paCket_dAta, TyPe from dfs.`pcapng/sniff.pcapng`";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    assertEquals(123, sets.rowCount());
    sets.clear();
  }

  @Test
  public void testWhereSyntaxQuery() throws Exception {
    String sql = "select type, src_ip, dst_ip, packet_length from dfs.`pcapng/sniff.pcapng` where src_ip= '10.2.15.239'";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    TupleMetadata schema = new SchemaBuilder()
        .addNullable("type", MinorType.VARCHAR)
        .addNullable("src_ip", MinorType.VARCHAR)
        .addNullable("dst_ip", MinorType.VARCHAR)
        .addNullable("packet_length", MinorType.INT)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("UDP", "10.2.15.239", "239.255.255.250", 214)
        .addRow("UDP", "10.2.15.239", "239.255.255.250", 214)
        .addRow("UDP", "10.2.15.239", "239.255.255.250", 214)
        .build();

    assertEquals(3, sets.rowCount());
    new RowSetComparison(expected).verifyAndClearAll(sets);
  }

  @Test
  public void testValidHeaders() throws Exception {
    String sql = "select * from dfs.`pcapng/sniff.pcapng`";
    RowSet sets = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata schema = new SchemaBuilder()
        .addNullable("timestamp", MinorType.TIMESTAMP)
        .addNullable("packet_length", MinorType.INT)
        .addNullable("type", MinorType.VARCHAR)
        .addNullable("src_ip", MinorType.VARCHAR)
        .addNullable("dst_ip", MinorType.VARCHAR)
        .addNullable("src_port", MinorType.INT)
        .addNullable("dst_port", MinorType.INT)
        .addNullable("src_mac_address", MinorType.VARCHAR)
        .addNullable("dst_mac_address", MinorType.VARCHAR)
        .addNullable("tcp_session", MinorType.BIGINT)
        .addNullable("tcp_ack", MinorType.INT)
        .addNullable("tcp_flags", MinorType.INT)
        .addNullable("tcp_flags_ns", MinorType.INT)
        .addNullable("tcp_flags_cwr", MinorType.INT)
        .addNullable("tcp_flags_ece", MinorType.INT)
        .addNullable("tcp_flags_ece_ecn_capable", MinorType.INT)
        .addNullable("tcp_flags_ece_congestion_experienced", MinorType.INT)
        .addNullable("tcp_flags_urg", MinorType.INT)
        .addNullable("tcp_flags_ack", MinorType.INT)
        .addNullable("tcp_flags_psh", MinorType.INT)
        .addNullable("tcp_flags_rst", MinorType.INT)
        .addNullable("tcp_flags_syn", MinorType.INT)
        .addNullable("tcp_flags_fin", MinorType.INT)
        .addNullable("tcp_parsed_flags", MinorType.VARCHAR)
        .addNullable("packet_data", MinorType.VARCHAR)
        .addNullable("captured_length", MinorType.INT)
        .addNullable("interface_id", MinorType.INT)
        .addNullable("interface_name", MinorType.VARCHAR)
        .addNullable("link_type", MinorType.INT)
        .addNullable("comment", MinorType.VARCHAR)
        .addNullable("direction", MinorType.VARCHAR)
        .addNullable("reception_type", MinorType.VARCHAR)
        .addNullable("fcs_length", MinorType.INT)
        .addNullable("drop_count", MinorType.BIGINT)
        .addNullable("packet_hash", MinorType.VARCHAR)
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();

    // parsed_data holds one map per registered decoder, so only its presence is pinned here
    TupleMetadata actual = sets.schema();
    assertEquals(schema.size() + 1, actual.size());
    for (int i = 0; i < schema.size(); i++) {
      ColumnMetadata column = schema.metadata(i);
      assertEquals(column.name(), column.majorType(), actual.metadata(column.name()).majorType());
    }
    assertTrue(actual.metadata("parsed_data").isMap());
    assertNotNull(actual.metadata("parsed_data").tupleSchema().metadata("echo_test"));
    sets.clear();
  }


  @Test
  public void testBigEndian() throws Exception {
    // The same capture written in both byte orders must decode identically.
    // (The upstream big-endian copy has zeroed timestamps, so they are not compared.)
    String sql = "select packet_length, src_ip, dst_ip, src_port, dst_port, src_mac_address from dfs.`pcapng/%s`";
    RowSet little = client.queryBuilder().sql(String.format(sql, "dhcp.pcapng")).rowSet();
    RowSet big = client.queryBuilder().sql(String.format(sql, "dhcp_big_endian.pcapng")).rowSet();

    assertEquals(4, little.rowCount());
    new RowSetComparison(little).verifyAndClearAll(big);
  }

  @Test
  public void testManyInterfaces() throws Exception {
    // 11 interfaces plus statistics and name resolution blocks between the packets
    String sql = "select count(*) from dfs.`pcapng/many_interfaces.pcapng`";
    assertEquals(64, queryBuilder().sql(sql).singletonLong());
  }

  /**
   * metadata.pcapng has one UDP packet from 10.0.0.N:100N per interface, each
   * on a different link type, plus a second big-endian section in which
   * interface 0 is raw IP. All timestamps are 2024-01-02T03:04:05.678Z except
   * eth1, which uses 2^-10 s units and if_tsoffset to give 03:04:05.500Z.
   */
  @Test
  public void testLinkTypesAndInterfaces() throws Exception {
    String sql = "select interface_id, link_type, interface_name, `timestamp`, src_ip, src_port, " +
        "src_mac_address, captured_length from dfs.`pcapng/metadata.pcapng`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata schema = new SchemaBuilder()
        .addNullable("interface_id", MinorType.INT)
        .addNullable("link_type", MinorType.INT)
        .addNullable("interface_name", MinorType.VARCHAR)
        .addNullable("timestamp", MinorType.TIMESTAMP)
        .addNullable("src_ip", MinorType.VARCHAR)
        .addNullable("src_port", MinorType.INT)
        .addNullable("src_mac_address", MinorType.VARCHAR)
        .addNullable("captured_length", MinorType.INT)
        .buildSchema();

    Instant ts = Instant.parse("2024-01-02T03:04:05.678Z");
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow(0, 1, "eth0", ts, "10.0.0.0", 1000, "02:00:00:00:00:01", 44)
        .addRow(1, 101, "tun0", ts, "10.0.0.1", 1001, null, 30)   // nanosecond resolution
        .addRow(2, 113, "any", ts, "10.0.0.2", 1002, null, 46)
        .addRow(3, 0, "lo0", ts, "10.0.0.3", 1003, null, 34)
        .addRow(4, 9, "ppp0", ts, "10.0.0.4", 1004, null, 34)
        .addRow(5, 276, "any2", ts, "10.0.0.5", 1005, null, 50)
        .addRow(6, 105, "wlan0", ts, null, null, null, 54)        // 802.11 is not decoded
        .addRow(7, 1, "eth1", Instant.parse("2024-01-02T03:04:05.500Z"), "10.0.0.7", 1007, "02:00:00:00:00:01", 44)
        .addRow(0, 101, "be0", ts, "10.0.0.9", 1009, null, 30)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  /**
   * encapsulations.pcapng: 802.11 (FromDS), radiotap + 802.11 QoS (ToDS) with TCP options,
   * an 802.1Q tag, QinQ + IPv6 with hop-by-hop and routing headers, an encrypted 802.11
   * frame, and an Ethernet frame with trailing padding.
   */
  @Test
  public void testEncapsulations() throws Exception {
    String sql = "select interface_name, type, src_ip, dst_ip, src_port, dst_port, src_mac_address, " +
        "dst_mac_address, tcp_flags from dfs.`pcapng/encapsulations.pcapng`";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata schema = new SchemaBuilder()
        .addNullable("interface_name", MinorType.VARCHAR)
        .addNullable("type", MinorType.VARCHAR)
        .addNullable("src_ip", MinorType.VARCHAR)
        .addNullable("dst_ip", MinorType.VARCHAR)
        .addNullable("src_port", MinorType.INT)
        .addNullable("dst_port", MinorType.INT)
        .addNullable("src_mac_address", MinorType.VARCHAR)
        .addNullable("dst_mac_address", MinorType.VARCHAR)
        .addNullable("tcp_flags", MinorType.INT)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("wlan-fromds", "UDP", "192.168.1.3", "192.168.1.1", 5353, 53, "02:00:00:00:00:03", "02:00:00:00:00:01", 0)
        .addRow("radiotap-qos", "TCP", "10.0.0.4", "10.0.0.5", 40000, 443, "02:00:00:00:00:04", "02:00:00:00:00:05", 24)
        .addRow("vlan", "UDP", "10.1.0.6", "10.1.0.7", 1111, 2222, "02:00:00:00:00:06", "02:00:00:00:00:07", 0)
        .addRow("qinq-ipv6", "TCP", "2001:db8:0:0:0:0:0:8", "2001:db8:0:0:0:0:0:9", 50000, 80, "02:00:00:00:00:08", "02:00:00:00:00:09", 24)
        .addRow("wlan-protected", null, null, null, null, null, null, null, null)
        .addRow("eth-padded", "UDP", "10.2.0.10", "10.2.0.11", 7, 9, "02:00:00:00:00:0A", "02:00:00:00:00:0B", 0)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSessionizedTcpStreams() throws Exception {
    // http.pcapng holds the same packets as pcap/http.pcap, so both readers must build the same sessions
    dirTestWatcher.copyResourceToRoot(Paths.get("pcap/http.pcap"), Paths.get("pcap/http.pcap"));
    String sql = "select * from table(dfs.`%s` (type => '%s', sessionizeTCPStreams => true))";
    RowSet pcap = client.queryBuilder().sql(String.format(sql, "pcap/http.pcap", "pcap")).rowSet();
    RowSet pcapng = client.queryBuilder().sql(String.format(sql, "pcapng/http.pcapng", "pcapng")).rowSet();

    assertEquals(2, pcapng.rowCount());
    new RowSetComparison(pcap).verifyAndClearAll(pcapng);

    sql = "select session_closed, data_volume_from_origin, data_volume_from_remote, substr(data_from_originator, 1, 27) as request " +
        "from table(dfs.`pcapng/http.pcapng` (type => 'pcapng', sessionizeTCPStreams => true))";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("session_closed", MinorType.BIT)
        .addNullable("data_volume_from_origin", MinorType.INT)
        .addNullable("data_volume_from_remote", MinorType.INT)
        .addNullable("request", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow(true, 479, 18364, "GET /download.html HTTP/1.1")
        // Still open when the capture ended
        .addRow(false, 721, 3020, "GET /pagead/ads?client=ca-p")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testPacketOptions() throws Exception {
    String sql = "select interface_name, comment, direction, reception_type, fcs_length, drop_count, packet_hash " +
        "from dfs.`pcapng/metadata.pcapng` where interface_name in ('eth0', 'eth1', 'tun0')";
    RowSet results = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata schema = new SchemaBuilder()
        .addNullable("interface_name", MinorType.VARCHAR)
        .addNullable("comment", MinorType.VARCHAR)
        .addNullable("direction", MinorType.VARCHAR)
        .addNullable("reception_type", MinorType.VARCHAR)
        .addNullable("fcs_length", MinorType.INT)
        .addNullable("drop_count", MinorType.BIGINT)
        .addNullable("packet_hash", MinorType.VARCHAR)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("eth0", "first packet", "inbound", "unicast", 4, 7L, "md5:000102030405060708090a0b0c0d0e0f")
        .addRow("tun0", null, null, null, null, null, null)
        .addRow("eth1", null, "outbound", null, null, null, null)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testGroupBy() throws Exception {
    String sql = "select src_ip, count(1), sum(packet_length) from dfs.`pcapng/sniff.pcapng` group by src_ip";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    assertEquals(47, sets.rowCount());
    sets.clear();
  }

  @Test
  public void testDistinctQuery() throws Exception {
    String sql = "select distinct `timestamp`, src_ip from dfs.`pcapng/sniff.pcapng`";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    assertEquals(119, sets.rowCount());
    sets.clear();
  }

  @Test(expected = UserRemoteException.class)
  public void testBasicQueryWithIncorrectFileName() throws Exception {
    String sql = "select * from dfs.`pcapng/drill.pcapng`";
    client.queryBuilder().sql(sql).rowSet();
  }

  @Test
  public void testPcapNGFileWithPcapExt() throws Exception {
    String sql = "select count(*) from dfs.`pcapng/example.pcap`";
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();

    assertEquals("Counts should match", 1, cnt);
  }

  @Test
  public void testInlineSchema() throws Exception {
    String sql =   "SELECT type, packet_length, `timestamp` FROM table(dfs.`pcapng/sniff.pcapng` " +
            "(type => 'pcapng', stat => false, sessionizeTCPStreams => false )) where type = 'ARP' limit 2";
    RowSet sets = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata schema = new SchemaBuilder()
            .addNullable("type", MinorType.VARCHAR)
            .addNullable("packet_length", MinorType.INT)
            .addNullable("timestamp", MinorType.TIMESTAMP)
            .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), schema)
            .addRow("ARP", 60, Instant.ofEpochMilli(1518010666140L))
            .addRow("ARP", 60, Instant.ofEpochMilli(1518010666140L))
            .build();

    assertEquals(2, sets.rowCount());
    new RowSetComparison(expected).verifyAndClearAll(sets);
  }
}
