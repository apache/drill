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
package org.apache.drill.exec.store.pcap.protocol.dhcp;

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

/** Query tests of the DHCP, DHCPv6 and TFTP decoders on the fixtures from fixtures/*_fixtures.py. */
public class TestAddressingDecoders extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("decoders/"));
  }

  @Test
  public void testDhcp() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.dhcp.message_type as message_type, " +
        "t.parsed_data.dhcp.client_mac as client_mac, t.parsed_data.dhcp.your_ip as your_ip, " +
        "t.parsed_data.dhcp.hostname as hostname, t.parsed_data.dhcp.lease_time as lease_time, " +
        "t.parsed_data.dhcp.dns_servers[1] as dns2, t.parsed_data.dhcp.options[0].code as first_option, " +
        "decode_error from dfs.`decoders/dhcp/dhcp.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("message_type", MinorType.VARCHAR)
        .addNullable("client_mac", MinorType.VARCHAR)
        .addNullable("your_ip", MinorType.VARCHAR)
        .addNullable("hostname", MinorType.VARCHAR)
        .addNullable("lease_time", MinorType.BIGINT)
        .addNullable("dns2", MinorType.VARCHAR)
        .addNullable("first_option", MinorType.INT)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("dhcp", "DISCOVER", "02:11:22:33:44:55", "0.0.0.0", "laptop", null, null, 53, null)
        .addRow("dhcp", "ACK", "02:11:22:33:44:55", "192.168.1.100", null, 86400L, "1.1.1.1", 53, null)
        .addRow(null, null, null, null, null, null, null, null, null)
        // Plain BOOTP, without the DHCP magic cookie
        .addRow(null, null, null, null, null, null, null, null, null)
        .addRow("dhcp", null, null, null, null, null, null, null, "dhcp: option 15 at offset 277 overruns the message")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testDhcpv6() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.dhcpv6.message_type as message_type, " +
        "t.parsed_data.dhcpv6.relayed_message_type as relayed, t.parsed_data.dhcpv6.transaction_id as xid, " +
        "t.parsed_data.dhcpv6.client_duid as client_duid, t.parsed_data.dhcpv6.ia_addresses[0] as address, " +
        "t.parsed_data.dhcpv6.domain_list[1] as domain2, t.parsed_data.dhcpv6.fqdn as fqdn, " +
        "decode_error from dfs.`decoders/dhcpv6/dhcpv6.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("message_type", MinorType.VARCHAR)
        .addNullable("relayed", MinorType.VARCHAR)
        .addNullable("xid", MinorType.INT)
        .addNullable("client_duid", MinorType.VARCHAR)
        .addNullable("address", MinorType.VARCHAR)
        .addNullable("domain2", MinorType.VARCHAR)
        .addNullable("fqdn", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    String duid = "000100012a2b2c2d021122334455";
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("dhcpv6", "SOLICIT", null, 0x5A1B2C, duid, null, null, "laptop.example.org", null)
        .addRow("dhcpv6", "REPLY", null, 0x5A1B2C, duid, "2001:db8:0:0:0:0:0:100", "corp.example", null, null)
        .addRow("dhcpv6", "RELAY-FORW", "SOLICIT", 0x5A1B2C, duid, null, null, "laptop.example.org", null)
        .addRow(null, null, null, null, null, null, null, null, null)
        .addRow("dhcpv6", null, null, null, null, null, null, null, "dhcpv6: option 24 at offset 100 overruns the message")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testTftp() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.tftp.opcode as opcode, t.parsed_data.tftp.filename as filename, " +
        "t.parsed_data.tftp.mode as `mode`, t.parsed_data.tftp.options[0].name as option_name, " +
        "t.parsed_data.tftp.options[0].`value` as option_value, t.parsed_data.tftp.error_code as error_code, " +
        "t.parsed_data.tftp.error_message as error_message, decode_error from dfs.`decoders/tftp/tftp.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("opcode", MinorType.VARCHAR)
        .addNullable("filename", MinorType.VARCHAR)
        .addNullable("mode", MinorType.VARCHAR)
        .addNullable("option_name", MinorType.VARCHAR)
        .addNullable("option_value", MinorType.VARCHAR)
        .addNullable("error_code", MinorType.INT)
        .addNullable("error_message", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("tftp", "RRQ", "boot/pxelinux.0", "octet", "blksize", "1428", null, null, null)
        .addRow("tftp", "WRQ", "config.txt", "netascii", null, null, null, null, null)
        .addRow("tftp", "ERROR", null, null, null, null, 1, "File not found", null)
        // An ACK between ephemeral ports is not seen by the decoder
        .addRow(null, null, null, null, null, null, null, null, null)
        .addRow(null, null, null, null, null, null, null, null, null)
        .addRow("tftp", null, null, null, null, null, null, null, "tftp: unterminated option value")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
