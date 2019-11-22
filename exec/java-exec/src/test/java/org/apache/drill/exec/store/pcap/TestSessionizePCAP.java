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


import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.joda.time.Period;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import org.junit.BeforeClass;
import org.junit.Test;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;

public class TestSessionizePCAP extends ClusterTest {

  private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

    PcapFormatConfig sampleConfig = new PcapFormatConfig();
    sampleConfig.sessionizeTCPStreams = true;

    cluster.defineFormat("cp", "pcap", sampleConfig);
    dirTestWatcher.copyResourceToRoot(Paths.get("store/pcap/"));
  }

  @Test
  public void testSessionizedStarQuery() throws Exception {
    String sql = "SELECT * FROM cp.`/store/pcap/attack-trace.pcap` WHERE src_port=1821 AND dst_port=445";

    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("session_start_time", "session_end_time", "session_duration", "total_packet_count", "connection_time", "src_ip", "dst_ip", "src_port", "dst_port",
        "src_mac_address", "dst_mac_address", "tcp_session", "is_corrupt", "data_from_originator", "data_from_remote", "data_volume_from_origin",
        "data_volume_from_remote", "packet_count_from_origin", "packet_count_from_remote")
      .baselineValues(LocalDateTime.parse("2009-04-20T03:28:28.374", formatter),
        LocalDateTime.parse("2009-04-20T03:28:28.508", formatter),
        Period.parse("PT0.134S"), 4,
        Period.parse("PT0.119S"),
        "98.114.205.102",
        "192.150.11.111",
        1821, 445,
        "00:08:E2:3B:56:01",
        "00:30:48:62:4E:4A",
        -8791568836279708938L,
        false,
        "........I....>...>..........Ib...<...<..........I....>...>", "", 62,0, 3, 1)
      .go();
  }

  @Test
  public void testSessionizedSpecificQuery() throws Exception {
    String sql = "SELECT session_start_time, session_end_time,session_duration, total_packet_count, connection_time, src_ip, dst_ip, src_port, dst_port, src_mac_address, dst_mac_address, tcp_session, " +
      "is_corrupt, data_from_originator, data_from_remote, data_volume_from_origin, data_volume_from_remote, packet_count_from_origin, packet_count_from_remote " +
      "FROM cp.`/store/pcap/attack-trace.pcap` WHERE src_port=1821 AND dst_port=445";

    testBuilder()
      .sqlQuery(sql)
      .ordered()
      .baselineColumns("session_start_time", "session_end_time", "session_duration", "total_packet_count", "connection_time", "src_ip", "dst_ip", "src_port", "dst_port",
        "src_mac_address", "dst_mac_address", "tcp_session", "is_corrupt", "data_from_originator", "data_from_remote", "data_volume_from_origin",
        "data_volume_from_remote", "packet_count_from_origin", "packet_count_from_remote")
      .baselineValues(LocalDateTime.parse("2009-04-20T03:28:28.374", formatter),
        LocalDateTime.parse("2009-04-20T03:28:28.508", formatter),
        Period.parse("PT0.134S"), 4,
        Period.parse("PT0.119S"),
        "98.114.205.102",
        "192.150.11.111",
        1821, 445,
        "00:08:E2:3B:56:01",
        "00:30:48:62:4E:4A",
        -8791568836279708938L,
        false,
        "........I....>...>..........Ib...<...<..........I....>...>", "", 62,0, 3, 1)
      .go();
  }

  @Test
  public void testSerDe() throws Exception {
    String sql = "SELECT COUNT(*) FROM cp.`/store/pcap/attack-trace.pcap`";
    String plan = queryBuilder().sql(sql).explainJson();
    long cnt = queryBuilder().physical(plan).singletonLong();
    assertEquals("Counts should match", 5L, cnt);
  }
}
