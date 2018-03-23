/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.pcap;

import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestPcapRecordReader extends BaseTestQuery {
  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("store", "pcap"));
  }

  @Test
  public void testStarQuery() throws Exception {
    runSQLVerifyCount("select * from dfs.`store/pcap/tcp-1.pcap`", 16);
    runSQLVerifyCount("select distinct DST_IP from dfs.`store/pcap/tcp-1.pcap`", 1);
    runSQLVerifyCount("select distinct DsT_IP from dfs.`store/pcap/tcp-1.pcap`", 1);
    runSQLVerifyCount("select distinct dst_ip from dfs.`store/pcap/tcp-1.pcap`", 1);
  }

  @Test
  public void testCountQuery() throws Exception {
    runSQLVerifyCount("select count(*) from dfs.`store/pcap/tcp-1.pcap`", 1);
    runSQLVerifyCount("select count(*) from dfs.`store/pcap/tcp-2.pcap`", 1);
  }

  @Test
  public void testDistinctQuery() throws Exception {
    // omit data field from distinct count for now
    runSQLVerifyCount("select distinct type, network, `timestamp`, src_ip, dst_ip, src_port, dst_port, src_mac_address, dst_mac_address, tcp_session, packet_length from dfs.`store/pcap/tcp-1.pcap`", 1);
  }

  @Test
  public void testFlagFormatting() {
    assertEquals("NS", Packet.formatFlags(0x100));
    assertEquals("CWR", Packet.formatFlags(0x80));
    assertEquals("ECE", Packet.formatFlags(0x40).substring(0, 3));
    assertEquals("ECE", Packet.formatFlags(0x42).substring(0, 3));
    assertEquals("URG", Packet.formatFlags(0x20));
    assertEquals("ACK", Packet.formatFlags(0x10));
    assertEquals("PSH", Packet.formatFlags(0x8));
    assertEquals("RST", Packet.formatFlags(0x4));
    assertEquals("SYN", Packet.formatFlags(0x2));
    assertEquals("FIN", Packet.formatFlags(0x1));
    assertEquals("RST|SYN|FIN", Packet.formatFlags(0x7));
  }

  @Test
  public void checkFlags() throws Exception {
    runSQLVerifyCount("select tcp_session, tcp_ack, tcp_flags from dfs.`store/pcap/synscan.pcap`", 2011);
  }

  private void runSQLVerifyCount(String sql, int expectedRowCount) throws Exception {
    List<QueryDataBatch> results = runSQLWithResults(sql);
    verifyRowCount(results, expectedRowCount);
  }

  private List<QueryDataBatch> runSQLWithResults(String sql) throws Exception {
    return testSqlWithResults(sql);
  }

  private void verifyRowCount(List<QueryDataBatch> results, int expectedRowCount) {
    int count = 0;
    for (final QueryDataBatch result : results) {
      count += result.getHeader().getRowCount();
      result.release();
    }
    assertEquals(expectedRowCount, count);
  }

}
