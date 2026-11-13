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
package org.apache.drill.exec.store.pcap.protocol.ldap;

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

/** Query tests of the LDAP session decoder on a generated capture. */
public class TestLdapSessionQueries extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("decoders/"));
  }

  private static String sessions(String options) {
    return "table(dfs.`decoders/ldap/ldap.pcapng` (type => 'pcapng', sessionizeTCPStreams => true" + options
        + ")) t";
  }

  @Test
  public void testLdap() throws Exception {
    String sql = "select src_port, parsed_protocol, t.parsed_data.ldap.version as version, " +
        "t.parsed_data.ldap.bind_dn as bind_dn, t.parsed_data.ldap.auth_type as auth_type, " +
        "t.parsed_data.ldap.password_present as password_present, " +
        "t.parsed_data.ldap.bind_result_code as bind_result_code, " +
        "t.parsed_data.ldap.bind_result as bind_result, " +
        "t.parsed_data.ldap.searches[0].base_dn as base_dn, t.parsed_data.ldap.searches[0].scope as scope, " +
        "t.parsed_data.ldap.searches[0].filter as filter, " +
        "t.parsed_data.ldap.entries_returned[0] as entry0, t.parsed_data.ldap.entries_returned[1] as entry1, " +
        "t.parsed_data.ldap.operation_count as operation_count, decode_error " +
        "from " + sessions("") + " order by src_port";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_port", MinorType.INT)
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("version", MinorType.INT)
        .addNullable("bind_dn", MinorType.VARCHAR)
        .addNullable("auth_type", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addNullable("bind_result_code", MinorType.INT)
        .addNullable("bind_result", MinorType.VARCHAR)
        .addNullable("base_dn", MinorType.VARCHAR)
        .addNullable("scope", MinorType.VARCHAR)
        .addNullable("filter", MinorType.VARCHAR)
        .addNullable("entry0", MinorType.VARCHAR)
        .addNullable("entry1", MinorType.VARCHAR)
        .addNullable("operation_count", MinorType.INT)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow(40001, "ldap", 3, "cn=admin,dc=example,dc=com", "simple", true, 0, "success",
            "dc=example,dc=com", "sub", "(&(objectClass=user)(sAMAccountName=admin))",
            "cn=alice,dc=example,dc=com", "cn=bob,dc=example,dc=com", 2, null)
        .addRow(40002, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
        .addRow(40003, "ldap", 3, "cn=svc,dc=example,dc=com", "simple", true, 49, "invalidCredentials",
            null, null, null, null, null, 1,
            "ldap: truncated message in client stream at byte 40")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testLdapExposedPassword() throws Exception {
    String sql = "select t.parsed_data.ldap.password as password from " + sessions(", exposeCredentials => true")
        + " where src_port = 40001";
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
