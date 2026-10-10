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
package org.apache.drill.exec.store.pcap.protocol.tls;

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

/** Query tests of the tls and stun decoders on fixtures from tls_fixtures.py and stun_fixtures.py. */
public class TestTlsAndStunQueries extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("decoders/"));
  }

  @Test
  public void testTls() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.tls.handshake_type as handshake_type, " +
        "t.parsed_data.tls.sni as sni, t.parsed_data.tls.alpn[0] as alpn, " +
        "t.parsed_data.tls.cipher_suite as cipher_suite, t.parsed_data.tls.ja3_hash as ja3_hash, " +
        "t.parsed_data.tls.ja3s_hash as ja3s_hash, decode_error from dfs.`decoders/tls/tls.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("handshake_type", MinorType.VARCHAR)
        .addNullable("sni", MinorType.VARCHAR)
        .addNullable("alpn", MinorType.VARCHAR)
        .addNullable("cipher_suite", MinorType.INT)
        .addNullable("ja3_hash", MinorType.VARCHAR)
        .addNullable("ja3s_hash", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("tls", "client_hello", "www.example.com", "h2", null, "cafe522b1f077145bca328ed52621259", null, null)
        .addRow("tls", "server_hello", null, null, 4865, null, "f4febc55ea12b31ae17cfb7e614afda8", null)
        .addRow(null, null, null, null, null, null, null, null)
        .addRow("tls", null, null, null, null, null, null, "tls: truncated client_hello: needs 32 bytes at offset 44")
        .addRow("tls", "client_hello", "www.example.com", null, null, null, null,
            "tls: handshake continues in a later segment")
        .addRow(null, null, null, null, null, null, null, null)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testTlsJa3String() throws Exception {
    String sql = "select t.parsed_data.tls.ja3 as ja3, t.parsed_data.tls.cipher_suites[0] as first_cipher " +
        "from dfs.`decoders/tls/tls.pcapng` t where t.parsed_data.tls.handshake_type = 'client_hello' " +
        "and decode_error is null";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("ja3", MinorType.VARCHAR)
        .addNullable("first_cipher", MinorType.INT)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        // The first cipher is GREASE (0x4a4a): kept in cipher_suites, left out of JA3
        .addRow("771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53," +
            "0-23-65281-10-11-35-16-5-13-51-45-43,29-23-24,0", 0x4a4a)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testTlsJa4() throws Exception {
    // JA4 of the fixture ClientHello, cross-checked with tls_fixtures.py#ja4_from_spec:
    //   a = t13d1512h2 (TLS 1.3 from supported_versions, SNI, 15 ciphers, 12 extensions, ALPN h2)
    //   b = sha256(sorted non-GREASE cipher hex)[:12]
    //   c = sha256(sorted non-GREASE extension hex without SNI/ALPN, '_', signature algorithms)[:12]
    String sql = "select t.parsed_data.tls.ja4 as ja4, t.parsed_data.tls.ja4_raw as ja4_raw " +
        "from dfs.`decoders/tls/tls.pcapng` t where t.parsed_data.tls.handshake_type = 'client_hello' " +
        "and decode_error is null";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("ja4", MinorType.VARCHAR)
        .addNullable("ja4_raw", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("t13d1512h2_8daaf6152771_2f4579fd44f8",
            "t13d1512h2_002f,0035,009c,009d,1301,1302,1303,c013,c014,c02b,c02c,c02f,c030,cca8,cca9" +
                "_0005,000a,000b,000d,0017,0023,002b,002d,0033,ff01_0403,0804,0401,0503")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testStun() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.stun.message_class as message_class, " +
        "t.parsed_data.stun.message_method as message_method, t.parsed_data.stun.username as username, " +
        "t.parsed_data.stun.xor_mapped_address as xor_mapped_address, " +
        "t.parsed_data.stun.error_code as error_code, decode_error from dfs.`decoders/stun/stun.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("message_class", MinorType.VARCHAR)
        .addNullable("message_method", MinorType.VARCHAR)
        .addNullable("username", MinorType.VARCHAR)
        .addNullable("xor_mapped_address", MinorType.VARCHAR)
        .addNullable("error_code", MinorType.INT)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("stun", "request", "binding", "evtj:h6vY", null, null, null)
        .addRow("stun", "success_response", "binding", null, "192.0.2.1:32853", null, null)
        .addRow("stun", "error_response", "allocate", null, null, 401, null)
        .addRow(null, null, null, null, null, null, null)
        .addRow("stun", null, null, null, null, null, "stun: attribute 0x8022 length 40 exceeds the message")
        .addRow("stun", "request", "binding", null, null, null, null)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testStunAttributes() throws Exception {
    String sql = "select t.parsed_data.stun.software as software, t.parsed_data.stun.attributes[1].type as type, " +
        "t.parsed_data.stun.attributes[1].length as len, t.parsed_data.stun.password_present as password_present " +
        "from dfs.`decoders/stun/stun.pcapng` t where t.parsed_data.stun.message_class = 'success_response'";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("software", MinorType.VARCHAR)
        .addNullable("type", MinorType.INT)
        .addNullable("len", MinorType.INT)
        .addNullable("password_present", MinorType.BIT)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("test vector", 0x0020, 8, false)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
