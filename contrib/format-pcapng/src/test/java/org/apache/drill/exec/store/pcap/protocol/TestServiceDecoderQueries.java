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
import java.time.Instant;

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

/** Query tests of the NTP, syslog, SSDP and SIP decoders on the fixtures under decoders/. */
public class TestServiceDecoderQueries extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("decoders/"));
  }

  @Test
  public void testNtp() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.ntp.mode as mode, t.parsed_data.ntp.stratum as stratum, " +
        "t.parsed_data.ntp.reference_id as reference_id, t.parsed_data.ntp.transmit_time as transmit_time, " +
        "t.parsed_data.ntp.request_code as request_code, decode_error from dfs.`decoders/ntp/ntp.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("mode", MinorType.VARCHAR)
        .addNullable("stratum", MinorType.INT)
        .addNullable("reference_id", MinorType.VARCHAR)
        .addNullable("transmit_time", MinorType.TIMESTAMP)
        .addNullable("request_code", MinorType.INT)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("ntp", "client", 0, null, Instant.ofEpochMilli(1704164645500L), null, null)
        .addRow("ntp", "server", 2, "192.168.1.1", Instant.ofEpochMilli(1704164646500L), null, null)
        .addRow(null, null, null, null, null, null, null)
        .addRow("ntp", "private", null, null, null, 42, null)
        .addRow("ntp", null, null, null, null, null, "ntp: truncated control message: count 100 exceeds 0 data bytes")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSyslog() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.syslog.facility_name as facility, " +
        "t.parsed_data.syslog.severity as severity, t.parsed_data.syslog.version as version, " +
        "t.parsed_data.syslog.hostname as hostname, t.parsed_data.syslog.app_name as app_name, " +
        "t.parsed_data.syslog.message as message, decode_error from dfs.`decoders/syslog/syslog.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("facility", MinorType.VARCHAR)
        .addNullable("severity", MinorType.INT)
        .addNullable("version", MinorType.INT)
        .addNullable("hostname", MinorType.VARCHAR)
        .addNullable("app_name", MinorType.VARCHAR)
        .addNullable("message", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("syslog", "local4", 5, 1, "mymachine.example.com", "evntslog", "An application event", null)
        .addRow("syslog", "auth", 6, null, "gateway", "sshd", "Accepted password for root from 10.0.0.9", null)
        .addRow(null, null, null, null, null, null, null, null)
        .addRow("syslog", null, null, null, null, null, null, "syslog: truncated RFC 5424 header")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSsdp() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.ssdp.method as method, " +
        "t.parsed_data.ssdp.status_code as status_code, t.parsed_data.ssdp.st as st, t.parsed_data.ssdp.nts as nts, " +
        "t.parsed_data.ssdp.server as server, t.parsed_data.ssdp.mx as mx, " +
        "t.parsed_data.ssdp.headers[0].name as first_header, decode_error from dfs.`decoders/ssdp/ssdp.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("method", MinorType.VARCHAR)
        .addNullable("status_code", MinorType.INT)
        .addNullable("st", MinorType.VARCHAR)
        .addNullable("nts", MinorType.VARCHAR)
        .addNullable("server", MinorType.VARCHAR)
        .addNullable("mx", MinorType.INT)
        .addNullable("first_header", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("ssdp", "M-SEARCH", null, "ssdp:all", null, null, 2, "HOST", null)
        .addRow("ssdp", null, 200, "upnp:rootdevice", null, "Linux/3.14 UPnP/1.0 IpBridge/1.26.0", null,
            "CACHE-CONTROL", null)
        .addRow("ssdp", "NOTIFY", null, null, "ssdp:byebye", null, null, "HOST", null)
        .addRow(null, null, null, null, null, null, null, null, null)
        .addRow("ssdp", null, null, null, null, null, null, null, "ssdp: malformed header line 2")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSip() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.sip.method as method, " +
        "t.parsed_data.sip.status_code as status_code, t.parsed_data.sip.call_id as call_id, " +
        "t.parsed_data.sip.to_address as to_address, " +
        "t.parsed_data.sip.via[0] as via, t.parsed_data.sip.username as username, " +
        "t.parsed_data.sip.content_length as content_length, decode_error from dfs.`decoders/sip/sip.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("method", MinorType.VARCHAR)
        .addNullable("status_code", MinorType.INT)
        .addNullable("call_id", MinorType.VARCHAR)
        .addNullable("to_address", MinorType.VARCHAR)
        .addNullable("via", MinorType.VARCHAR)
        .addNullable("username", MinorType.VARCHAR)
        .addNullable("content_length", MinorType.BIGINT)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("sip", "INVITE", null, "a84b4c76e66710@pc33.atlanta.example.com", "Bob <sip:bob@biloxi.example.com>",
            "SIP/2.0/UDP pc33.atlanta.example.com;branch=z9hG4bK776asdhds", "alice", 4L, null)
        .addRow("sip", null, 180, "a84b4c76e66710@pc33.atlanta.example.com", "Bob <sip:bob@biloxi.example.com>;tag=a6c85cf",
            "SIP/2.0/TCP pc33.atlanta.example.com;branch=z9hG4bK776asdhds", null, 0L, null)
        .addRow(null, null, null, null, null, null, null, null, null)
        .addRow("sip", null, null, null, null, null, null, null, "sip: malformed header line 2")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
