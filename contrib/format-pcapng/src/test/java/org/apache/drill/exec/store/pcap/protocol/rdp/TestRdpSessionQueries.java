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
package org.apache.drill.exec.store.pcap.protocol.rdp;

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

/** Query tests of the RDP session decoder on a generated capture. */
public class TestRdpSessionQueries extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("decoders/"));
  }

  private static String sessions() {
    return "table(dfs.`decoders/rdp/rdp.pcapng` (type => 'pcapng', sessionizeTCPStreams => true)) t";
  }

  @Test
  public void testRdp() throws Exception {
    String sql = "select src_port, parsed_protocol, t.parsed_data.rdp.cookie as cookie, "
        + "t.parsed_data.rdp.requested_protocols[0] as proto0, t.parsed_data.rdp.requested_protocols[1] as proto1, "
        + "t.parsed_data.rdp.selected_protocol as selected, t.parsed_data.rdp.negotiation_failure as failure, "
        + "decode_error from " + sessions() + " order by src_port";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_port", MinorType.INT)
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("cookie", MinorType.VARCHAR)
        .addNullable("proto0", MinorType.VARCHAR)
        .addNullable("proto1", MinorType.VARCHAR)
        .addNullable("selected", MinorType.VARCHAR)
        .addNullable("failure", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow(51001, "rdp", "alice", "TLS", "CredSSP", "TLS", null, null)
        .addRow(51002, null, null, null, null, null, null, null)
        .addRow(51003, "rdp", null, null, null, null, null, "rdp: truncated negotiation request")
        .addRow(51004, "rdp", null, "RDP", null, null, "HYBRID_REQUIRED_BY_SERVER", null)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
