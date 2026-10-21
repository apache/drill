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
package org.apache.drill.exec.store.pcap.protocol;

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

/** Queries over the NetBIOS-NS, RADIUS and SNMP fixtures in decoders/. */
public class TestNetbiosRadiusSnmpQueries extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("decoders/"));
  }

  @Test
  public void testNetbiosNs() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.netbios_ns.opcode as opcode, " +
        "t.parsed_data.netbios_ns.broadcast as broadcast, " +
        "t.parsed_data.netbios_ns.questions[0].name as qname, " +
        "t.parsed_data.netbios_ns.questions[0].suffix_name as suffix_name, " +
        "t.parsed_data.netbios_ns.answers[0].addresses[0] as address, " +
        "t.parsed_data.netbios_ns.answers[0].node_type as node_type, decode_error " +
        "from dfs.`decoders/netbios_ns/netbios_ns.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("opcode", MinorType.VARCHAR)
        .addNullable("broadcast", MinorType.BIT)
        .addNullable("qname", MinorType.VARCHAR)
        .addNullable("suffix_name", MinorType.VARCHAR)
        .addNullable("address", MinorType.VARCHAR)
        .addNullable("node_type", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("netbios_ns", "query", true, "FILESRV", "file_server", null, null, null)
        .addRow("netbios_ns", "query", false, null, null, "10.0.0.7", "H", null)
        .addRow(null, null, null, null, null, null, null, null)
        .addRow("netbios_ns", null, null, null, null, null, null,
            "netbios_ns: truncated answer 1: needs 6 bytes at offset 56")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testRadius() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.radius.code_name as code_name, " +
        "t.parsed_data.radius.username as username, t.parsed_data.radius.password_present as password_present, " +
        "t.parsed_data.radius.nas_ip_address as nas_ip, t.parsed_data.radius.nas_port as nas_port, " +
        "t.parsed_data.radius.acct_status_type as status, t.parsed_data.radius.reply_message as reply, " +
        "t.parsed_data.radius.attributes[1].`value` as password_value, decode_error " +
        "from dfs.`decoders/radius/radius.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("code_name", MinorType.VARCHAR)
        .addNullable("username", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addNullable("nas_ip", MinorType.VARCHAR)
        .addNullable("nas_port", MinorType.BIGINT)
        .addNullable("status", MinorType.VARCHAR)
        .addNullable("reply", MinorType.VARCHAR)
        .addNullable("password_value", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("radius", "Access-Request", "alice", true, "10.0.0.2", 3L, null, null, null, null)
        .addRow("radius", "Access-Accept", null, false, null, null, null, "Welcome", null, null)
        .addRow("radius", "Accounting-Request", null, false, null, null, "Start", null, "532d31", null)
        .addRow(null, null, null, null, null, null, null, null, null, null)
        .addRow("radius", null, null, null, null, null, null, null, null, "radius: truncated: length 83 but 79 bytes")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSnmp() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.snmp.version as version, " +
        "t.parsed_data.snmp.pdu_type as pdu_type, t.parsed_data.snmp.request_id as request_id, " +
        "t.parsed_data.snmp.community_present as community_present, t.parsed_data.snmp.community as community, " +
        "t.parsed_data.snmp.varbinds[0].oid as oid, t.parsed_data.snmp.varbinds[1].`value` as uptime, " +
        "t.parsed_data.snmp.agent_address as agent, t.parsed_data.snmp.msg_user_name as user_name, " +
        "t.parsed_data.snmp.security_level as security_level, t.parsed_data.snmp.encrypted as encrypted, " +
        "decode_error from dfs.`decoders/snmp/snmp.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("version", MinorType.VARCHAR)
        .addNullable("pdu_type", MinorType.VARCHAR)
        .addNullable("request_id", MinorType.BIGINT)
        .addNullable("community_present", MinorType.BIT)
        .addNullable("community", MinorType.VARCHAR)
        .addNullable("oid", MinorType.VARCHAR)
        .addNullable("uptime", MinorType.VARCHAR)
        .addNullable("agent", MinorType.VARCHAR)
        .addNullable("user_name", MinorType.VARCHAR)
        .addNullable("security_level", MinorType.VARCHAR)
        .addNullable("encrypted", MinorType.BIT)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    String sysDescr = "1.3.6.1.2.1.1.1.0";
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("snmp", "v2c", "get-request", 1234L, true, null, sysDescr, null, null, null, null, null, null)
        .addRow("snmp", "v2c", "get-response", 1234L, true, null, sysDescr, "123456", null, null, null, null, null)
        .addRow("snmp", "v1", "trap", null, true, null, sysDescr, null, "10.0.0.9", null, null, null, null)
        .addRow("snmp", "v3", null, null, false, null, null, null, null, "carol", "authPriv", true, null)
        .addRow(null, null, null, null, null, null, null, null, null, null, null, null, null)
        .addRow("snmp", null, null, null, null, null, null, null, null, null, null, null,
            "snmp: truncated: message length 53 but 50 bytes")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSnmpCommunityWithExposeCredentials() throws Exception {
    String sql = "select t.parsed_data.snmp.community as community from table(dfs.`decoders/snmp/snmp.pcapng` " +
        "(type => 'pcapng', exposeCredentials => true)) t where t.parsed_data.snmp.version in ('v1', 'v2c')";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("community", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("public")
        .addRow("public")
        .addRow("traps")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
