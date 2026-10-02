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
package org.apache.drill.exec.store.pcap.protocol.quic;

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

/** Query tests of the quic decoder on the fixture from quic_fixtures.py. */
public class TestQuicQueries extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("decoders/"));
  }

  @Test
  public void testQuic() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.quic.packet_type as packet_type, " +
        "t.parsed_data.quic.version as version, t.parsed_data.quic.dcid as dcid, " +
        "t.parsed_data.quic.sni as sni, t.parsed_data.quic.alpn[0] as alpn, " +
        "t.parsed_data.quic.cipher_suites[0] as first_cipher, t.parsed_data.quic.ja4 as ja4, " +
        "decode_error from dfs.`decoders/quic/quic.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("packet_type", MinorType.VARCHAR)
        .addNullable("version", MinorType.VARCHAR)
        .addNullable("dcid", MinorType.VARCHAR)
        .addNullable("sni", MinorType.VARCHAR)
        .addNullable("alpn", MinorType.VARCHAR)
        .addNullable("first_cipher", MinorType.INT)
        .addNullable("ja4", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("quic", "initial", "00000001", "8394c8f03e515708", "example.org", "h2", 0x1301,
            "q13d0305h2_55b375c5d22e_beb9f91c6f80", null)
        .addRow(null, null, null, null, null, null, null, null, null)
        .addRow("quic", null, null, null, null, null, null, null,
            "quic: packet length 1179 exceeds the datagram")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
