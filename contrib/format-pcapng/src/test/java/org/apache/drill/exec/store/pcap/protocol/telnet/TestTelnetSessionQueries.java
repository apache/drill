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
package org.apache.drill.exec.store.pcap.protocol.telnet;

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

/** Query tests of the Telnet session decoder on a generated capture. */
public class TestTelnetSessionQueries extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("decoders/"));
  }

  private static String sessions(String options) {
    return "table(dfs.`decoders/telnet/telnet.pcapng` (type => 'pcapng', sessionizeTCPStreams => true"
        + options + ")) t";
  }

  @Test
  public void testTelnet() throws Exception {
    String sql = "select src_port, parsed_protocol, t.parsed_data.telnet.client_text as client_text, "
        + "t.parsed_data.telnet.terminal_type as terminal_type, t.parsed_data.telnet.login_name as login_name, "
        + "t.parsed_data.telnet.password_present as password_present, t.parsed_data.telnet.password as password, "
        + "t.parsed_data.telnet.options[0].option as opt0, t.parsed_data.telnet.options[0].negotiation as neg0, "
        + "decode_error from " + sessions("") + " order by src_port";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_port", MinorType.INT)
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("client_text", MinorType.VARCHAR)
        .addNullable("terminal_type", MinorType.VARCHAR)
        .addNullable("login_name", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addNullable("password", MinorType.VARCHAR)
        .addNullable("opt0", MinorType.VARCHAR)
        .addNullable("neg0", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow(50001, "telnet", "alice\r\nsecret\r\n", "xterm", "alice", true, null, "TERMINAL_TYPE", "WILL", null)
        .addRow(50002, null, null, null, null, null, null, null, null, null)
        .addRow(50003, "telnet", "", null, null, false, null, null, null,
            "telnet: unterminated subnegotiation in client stream")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testTelnetExposedPassword() throws Exception {
    String sql = "select t.parsed_data.telnet.password as password from "
        + sessions(", exposeCredentials => true") + " where src_port = 50001";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("password", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("secret")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
