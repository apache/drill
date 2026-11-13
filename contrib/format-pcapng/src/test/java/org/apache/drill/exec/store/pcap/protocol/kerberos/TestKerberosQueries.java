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
package org.apache.drill.exec.store.pcap.protocol.kerberos;

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

/** Queries over the Kerberos packet fixture in decoders/. */
public class TestKerberosQueries extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("decoders/"));
  }

  @Test
  public void testKerberos() throws Exception {
    String sql = "select parsed_protocol, t.parsed_data.kerberos.message_type as message_type, " +
        "t.parsed_data.kerberos.realm as realm, t.parsed_data.kerberos.client_name as client_name, " +
        "t.parsed_data.kerberos.server_name as server_name, " +
        "t.parsed_data.kerberos.encryption_types[0] as first_etype, " +
        "t.parsed_data.kerberos.ticket_encryption_type as ticket_etype, " +
        "t.parsed_data.kerberos.error_code as error_code, " +
        "t.parsed_data.kerberos.error_text as error_text, " +
        "t.parsed_data.kerberos.pre_auth_present as pre_auth, decode_error " +
        "from dfs.`decoders/kerberos/kerberos.pcapng` t";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("message_type", MinorType.VARCHAR)
        .addNullable("realm", MinorType.VARCHAR)
        .addNullable("client_name", MinorType.VARCHAR)
        .addNullable("server_name", MinorType.VARCHAR)
        .addNullable("first_etype", MinorType.INT)
        .addNullable("ticket_etype", MinorType.INT)
        .addNullable("error_code", MinorType.INT)
        .addNullable("error_text", MinorType.VARCHAR)
        .addNullable("pre_auth", MinorType.BIT)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("kerberos", "AS-REQ", "EXAMPLE.COM", "alice", "krbtgt/EXAMPLE.COM", 18, null, null, null, true, null)
        .addRow("kerberos", "AS-REP", "EXAMPLE.COM", "alice", "HTTP/web.example.com", null, 23, null, null, false,
            null)
        .addRow("kerberos", "KRB-ERROR", "EXAMPLE.COM", null, "krbtgt/EXAMPLE.COM", null, null, 25,
            "KDC_ERR_PREAUTH_REQUIRED", null, null)
        .addRow(null, null, null, null, null, null, null, null, null, null, null)
        .addRow("kerberos", null, null, null, null, null, null, null, null, null,
            "kerberos: length 144 at offset 0 overruns its container")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
