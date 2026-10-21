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
package org.apache.drill.exec.store.pcap.protocol.smb;

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

/** Query tests of the SMB2/SMB3 session decoder on a generated capture (decoders/smb/smb.pcapng). */
public class TestSmbSessionQueries extends ClusterTest {

  private static final String CLIENT_GUID = "03020100-0504-0706-0809-0a0b0c0d0e0f";
  private static final String SERVER_GUID = "13121110-1514-1716-1819-1a1b1c1d1e1f";

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("decoders/"));
  }

  private static String sessions() {
    return "table(dfs.`decoders/smb/smb.pcapng` (type => 'pcapng', sessionizeTCPStreams => true)) t";
  }

  @Test
  public void testSmbSessions() throws Exception {
    String sql = "select src_port, parsed_protocol, t.parsed_data.smb.dialect as dialect, "
        + "t.parsed_data.smb.client_dialects[3] as dialect4, "
        + "t.parsed_data.smb.signing_required as signing, t.parsed_data.smb.encryption as encryption, "
        + "t.parsed_data.smb.server_guid as server_guid, t.parsed_data.smb.client_guid as client_guid, "
        + "t.parsed_data.smb.auth_type as auth_type, t.parsed_data.smb.user_name as user_name, "
        + "t.parsed_data.smb.domain_name as domain_name, t.parsed_data.smb.workstation as workstation, "
        + "t.parsed_data.smb.ntlm_version as ntlm_version, decode_error from " + sessions() + " order by src_port";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_port", MinorType.INT)
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("dialect", MinorType.VARCHAR)
        .addNullable("dialect4", MinorType.VARCHAR)
        .addNullable("signing", MinorType.BIT)
        .addNullable("encryption", MinorType.BIT)
        .addNullable("server_guid", MinorType.VARCHAR)
        .addNullable("client_guid", MinorType.VARCHAR)
        .addNullable("auth_type", MinorType.VARCHAR)
        .addNullable("user_name", MinorType.VARCHAR)
        .addNullable("domain_name", MinorType.VARCHAR)
        .addNullable("workstation", MinorType.VARCHAR)
        .addNullable("ntlm_version", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        // 40501: full SMB 3.1.1 session with an NTLMv2 SESSION_SETUP
        .addRow(40501, "smb", "3.1.1", "3.1.1", true, false, SERVER_GUID, CLIENT_GUID, "ntlmssp",
            "alice", "CORP", "WS01", "NTLMv2", null)
        // 40502: HTTP on port 445, left undecoded
        .addRow(40502, null, null, null, null, null, null, null, null, null, null, null, null, null)
        // 40503: the server SMB2 message is cut short before a full header
        .addRow(40503, "smb", null, null, null, false, null, CLIENT_GUID, null, null, null, null, null,
            "smb: truncated SMB2 header in server stream at byte 4")
        // 40504: Kerberos SESSION_SETUP: no NTLM identity, auth_type kerberos
        .addRow(40504, "smb", "3.1.1", null, true, false, SERVER_GUID, CLIENT_GUID, "kerberos",
            null, null, null, null, null)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
