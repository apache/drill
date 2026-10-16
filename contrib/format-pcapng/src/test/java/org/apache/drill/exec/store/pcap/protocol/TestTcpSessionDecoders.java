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

/** Query tests of the FTP, SSH and DNS-over-TCP session decoders on generated captures. */
public class TestTcpSessionDecoders extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("decoders/"));
  }

  private static String sessions(String file, String options) {
    return "table(dfs.`decoders/" + file + "` (type => 'pcapng', sessionizeTCPStreams => true" + options + ")) t";
  }

  @Test
  public void testFtp() throws Exception {
    String sql = "select src_port, parsed_protocol, t.parsed_data.ftp.banner as banner, "
        + "t.parsed_data.ftp.username as username, t.parsed_data.ftp.password_present as password_present, "
        + "t.parsed_data.ftp.password as password, t.parsed_data.ftp.tls_started as tls, "
        + "t.parsed_data.ftp.system_type as syst, t.parsed_data.ftp.current_directories[1] as dir, "
        + "t.parsed_data.ftp.transfers[0].data_address as data_address, "
        + "t.parsed_data.ftp.transfers[1].reply_code as stor_code, t.parsed_data.ftp.commands[1].argument as pass_arg, "
        + "decode_error from " + sessions("ftp/ftp.pcapng", "") + " order by src_port";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_port", MinorType.INT)
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("banner", MinorType.VARCHAR)
        .addNullable("username", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addNullable("password", MinorType.VARCHAR)
        .addNullable("tls", MinorType.BIT)
        .addNullable("syst", MinorType.VARCHAR)
        .addNullable("dir", MinorType.VARCHAR)
        .addNullable("data_address", MinorType.VARCHAR)
        .addNullable("stor_code", MinorType.INT)
        .addNullable("pass_arg", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow(40001, "ftp", "Welcome\nFTP ready", "anonymous", true, null, false, "UNIX Type: L8", "/pub",
            "10.0.0.2:50000", 553, "***", null)
        .addRow(40002, null, null, null, null, null, null, null, null, null, null, null, null)
        .addRow(40003, "ftp", "ready", "bob", false, null, false, null, null, null, null, null,
            "ftp: unparseable data in server stream at byte 11")
        .addRow(40004, "ftp", "ready", null, false, null, true, null, null, null, null, null, null)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testFtpExposedPassword() throws Exception {
    String sql = "select t.parsed_data.ftp.password as password, t.parsed_data.ftp.commands[1].argument as pass_arg from "
        + sessions("ftp/ftp.pcapng", ", exposeCredentials => true") + " where src_port = 40001";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("password", MinorType.VARCHAR)
        .addNullable("pass_arg", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("guest@example.com", "guest@example.com")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSsh() throws Exception {
    // HASSH values computed with hashlib by ssh_fixtures.py
    String sql = "select src_port, parsed_protocol, t.parsed_data.ssh.client_software as client_software, "
        + "t.parsed_data.ssh.client_comments as client_comments, t.parsed_data.ssh.server_software as server_software, "
        + "t.parsed_data.ssh.client_kex_algorithms[0] as kex, t.parsed_data.ssh.server_ciphers[1] as server_cipher, "
        + "t.parsed_data.ssh.hassh as hassh, t.parsed_data.ssh.hassh_server as hassh_server, decode_error from "
        + sessions("ssh/ssh.pcapng", "") + " order by src_port";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_port", MinorType.INT)
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("client_software", MinorType.VARCHAR)
        .addNullable("client_comments", MinorType.VARCHAR)
        .addNullable("server_software", MinorType.VARCHAR)
        .addNullable("kex", MinorType.VARCHAR)
        .addNullable("server_cipher", MinorType.VARCHAR)
        .addNullable("hassh", MinorType.VARCHAR)
        .addNullable("hassh_server", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    String hassh = "758d6483ea418a9d7ce3eaa2b5f911ae";
    String hasshServer = "4245702d7c71099e45efc16747f1a9f8";
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow(40101, "ssh", "OpenSSH_9.6p1", "Ubuntu-3ubuntu13.5", "OpenSSH_8.9p1", "curve25519-sha256",
            "aes256-gcm@openssh.com", hassh, hasshServer, null)
        .addRow(40102, null, null, null, null, null, null, null, null, null)
        .addRow(40103, "ssh", "OpenSSH_9.6p1", "Ubuntu-3ubuntu13.5", "OpenSSH_8.9p1", null, null, null, null,
            "ssh: truncated KEXINIT in client stream")
        .addRow(40104, "ssh", "OpenSSH_9.6p1", "Ubuntu-3ubuntu13.5", "dropbear_2022.83", "curve25519-sha256",
            "aes256-gcm@openssh.com", hassh, hasshServer, null)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testDnsOverTcp() throws Exception {
    String sql = "select src_port, parsed_protocol, t.parsed_data.dns.queries[1].name as q2, "
        + "t.parsed_data.dns.answers[1].data as a2, t.parsed_data.dns.answers[63].name as a64, "
        + "t.parsed_data.dns.client_message_count as client_messages, "
        + "t.parsed_data.dns.server_message_count as server_messages, "
        + "t.parsed_data.dns.is_zone_transfer as zone_transfer, decode_error from "
        + sessions("dns_tcp/dns_tcp.pcapng", "") + " order by src_port";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_port", MinorType.INT)
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("q2", MinorType.VARCHAR)
        .addNullable("a2", MinorType.VARCHAR)
        .addNullable("a64", MinorType.VARCHAR)
        .addNullable("client_messages", MinorType.INT)
        .addNullable("server_messages", MinorType.INT)
        .addNullable("zone_transfer", MinorType.BIT)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow(40201, "dns", "example.org", "2606:2800:220:1:0:0:0:1", null, 2, 2, false, null)
        .addRow(40202, null, null, null, null, null, null, null, null)
        .addRow(40203, "dns", null, null, null, 1, 0, false,
            "dns: server message 1: truncated answer 1: needs 4 bytes at offset 52")
        .addRow(40204, "dns", null, "10.1.0.1", "host63.zone.test", 1, 2, true, "dns: answers truncated to 64")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
