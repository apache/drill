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

import java.nio.file.Paths;
import java.time.Instant;

import org.apache.drill.categories.RowSetTests;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
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

@Category(RowSetTests.class)
public class TestPcapngRecordReader extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("store", "pcapng"));
  }

  @Test
  public void testStarQuery() throws Exception {
    String sql = "select * from dfs.`store/pcapng/sniff.pcapng`";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    assertEquals(123, sets.rowCount());
    sets.clear();
  }

  @Test
  public void testExplicitQuery() throws Exception {
    String sql = "select type, packet_length, `timestamp` from dfs.`store/pcapng/sniff.pcapng` where type = 'ARP'";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    TupleMetadata schema = new SchemaBuilder()
        .addNullable("type", MinorType.VARCHAR)
        .add("packet_length", MinorType.INT)
        .add("timestamp", MinorType.TIMESTAMP)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("ARP", 90, Instant.ofEpochMilli(1518010669927L))
        .addRow("ARP", 90, Instant.ofEpochMilli(1518010671874L))
        .build();

    assertEquals(2, sets.rowCount());
    new RowSetComparison(expected).verifyAndClearAll(sets);
  }

  @Test
  public void testLimitPushdown() throws Exception {
    String sql = "select * from dfs.`store/pcapng/sniff.pcapng` where type = 'UDP' limit 10 offset 65";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    assertEquals(6, sets.rowCount());
    sets.clear();
  }

  @Test
  public void testSerDe() throws Exception {
    String sql = "select count(*) from dfs.`store/pcapng/example.pcapng`";
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();

    assertEquals("Counts should match", 1, cnt);
  }

  @Test
  public void testExplicitQueryWithCompressedFile() throws Exception {
    QueryTestUtil.generateCompressedFile("store/pcapng/sniff.pcapng", "zip", "store/pcapng/sniff.pcapng.zip");
    String sql = "select type, packet_length, `timestamp` from dfs.`store/pcapng/sniff.pcapng.zip` where type = 'ARP'";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    TupleMetadata schema = new SchemaBuilder()
        .addNullable("type", MinorType.VARCHAR)
        .add("packet_length", MinorType.INT)
        .add("timestamp", MinorType.TIMESTAMP)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("ARP", 90, Instant.ofEpochMilli(1518010669927L))
        .addRow("ARP", 90, Instant.ofEpochMilli(1518010671874L))
        .build();

    assertEquals(2, sets.rowCount());
    new RowSetComparison(expected).verifyAndClearAll(sets);
  }

  @Test
  public void testCaseInsensitiveQuery() throws Exception {
    String sql = "select `timestamp`, paCket_dAta, TyPe from dfs.`store/pcapng/sniff.pcapng`";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    assertEquals(123, sets.rowCount());
    sets.clear();
  }

  @Test
  public void testWhereSyntaxQuery() throws Exception {
    String sql = "select type, src_ip, dst_ip, packet_length from dfs.`store/pcapng/sniff.pcapng` where src_ip= '10.2.15.239'";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    TupleMetadata schema = new SchemaBuilder()
        .addNullable("type", MinorType.VARCHAR)
        .addNullable("src_ip", MinorType.VARCHAR)
        .addNullable("dst_ip", MinorType.VARCHAR)
        .add("packet_length", MinorType.INT)
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
    String sql = "select * from dfs.`store/pcapng/sniff.pcapng`";
    RowSet sets = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata schema = new SchemaBuilder()
        .add("timestamp", MinorType.TIMESTAMP)
        .add("packet_length", MinorType.INT)
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
        .build();

    RowSet expected = new RowSetBuilder(client.allocator(), schema).build();
    new RowSetComparison(expected).verifyAndClearAll(sets);
  }

  @Test
  public void testGroupBy() throws Exception {
    String sql = "select src_ip, count(1), sum(packet_length) from dfs.`store/pcapng/sniff.pcapng` group by src_ip";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    assertEquals(47, sets.rowCount());
    sets.clear();
  }

  @Test
  public void testDistinctQuery() throws Exception {
    String sql = "select distinct `timestamp`, src_ip from dfs.`store/pcapng/sniff.pcapng`";
    QueryBuilder builder = client.queryBuilder().sql(sql);
    RowSet sets = builder.rowSet();

    assertEquals(119, sets.rowCount());
    sets.clear();
  }

  @Test(expected = UserRemoteException.class)
  public void testBasicQueryWithIncorrectFileName() throws Exception {
    String sql = "select * from dfs.`store/pcapng/drill.pcapng`";
    client.queryBuilder().sql(sql).rowSet();
  }
}