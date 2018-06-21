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
package org.apache.drill.exec.store.pcapng;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;

public class TestPcapngHeaders extends ClusterTest {
  @BeforeClass
  public static void setupTestFiles() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher).maxParallelization(1));
    dirTestWatcher.copyResourceToRoot(Paths.get("store", "pcapng"));
  }

  @Test
  public void testValidHeadersForStarQuery() throws IOException {
    String query = "select * from dfs.`store/pcapng/sniff.pcapng`";
    RowSet actual = client.queryBuilder().sql(query).rowSet();

    TupleSchema expectedSchema = new TupleSchema();

    expectedSchema.add(MaterializedField.create("tcp_flags_ece_ecn_capable", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("tcp_flags_ece_congestion_experienced", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("tcp_flags_psh", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("type", Types.optional(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(MaterializedField.create("tcp_flags_cwr", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("dst_ip", Types.optional(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(MaterializedField.create("src_ip", Types.optional(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(MaterializedField.create("tcp_flags_fin", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("tcp_flags_ece", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("tcp_flags", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("tcp_flags_ack", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("src_mac_address", Types.optional(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(MaterializedField.create("tcp_flags_syn", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("tcp_flags_rst", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("timestamp", Types.required(TypeProtos.MinorType.TIMESTAMP)));
    expectedSchema.add(MaterializedField.create("tcp_session", Types.optional(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(MaterializedField.create("packet_data", Types.optional(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(MaterializedField.create("tcp_parsed_flags", Types.optional(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(MaterializedField.create("tcp_flags_ns", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("src_port", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("packet_length", Types.required(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("tcp_flags_urg", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("tcp_ack", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("dst_port", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("dst_mac_address", Types.optional(TypeProtos.MinorType.VARCHAR)));

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(actual);
  }

  @Test
  public void testValidHeadersForProjection() throws IOException {
    String query = "select sRc_ip, dst_IP, dst_mAc_address, src_Port, tcp_session, `Timestamp`  from dfs.`store/pcapng/sniff.pcapng`";
    RowSet actual = client.queryBuilder().sql(query).rowSet();

    TupleSchema expectedSchema = new TupleSchema();

    expectedSchema.add(MaterializedField.create("sRc_ip", Types.optional(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(MaterializedField.create("dst_IP", Types.optional(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(MaterializedField.create("dst_mAc_address", Types.optional(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(MaterializedField.create("src_Port", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("tcp_session", Types.optional(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(MaterializedField.create("Timestamp", Types.required(TypeProtos.MinorType.TIMESTAMP)));

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(actual);
  }

  @Test
  public void testValidHeadersForMissColumns() throws IOException {
    String query = "select `timestamp`, `name`, `color` from dfs.`store/pcapng/sniff.pcapng`";
    RowSet actual = client.queryBuilder().sql(query).rowSet();

    TupleSchema expectedSchema = new TupleSchema();

    expectedSchema.add(MaterializedField.create("timestamp", Types.required(TypeProtos.MinorType.TIMESTAMP)));
    expectedSchema.add(MaterializedField.create("name", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("color", Types.optional(TypeProtos.MinorType.INT)));

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(actual);
  }

  @Test
  public void testMixColumns() throws IOException {
    String query = "select src_ip, dst_ip, dst_mac_address, src_port, tcp_session, `timestamp`  from dfs.`store/pcapng/sniff.pcapng`";
    RowSet actual = client.queryBuilder().sql(query).rowSet();

    TupleSchema expectedSchema = new TupleSchema();

    expectedSchema.add(MaterializedField.create("sRc_ip", Types.optional(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(MaterializedField.create("dst_IP", Types.optional(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(MaterializedField.create("dst_mAc_address", Types.optional(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(MaterializedField.create("src_Port", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("tcp_session", Types.optional(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(MaterializedField.create("Timestamp", Types.required(TypeProtos.MinorType.TIMESTAMP)));

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(actual);

    String queryWithDiffOrder = "select `timestamp`, src_ip, dst_ip, src_port, tcp_session, dst_mac_address from dfs.`store/pcapng/sniff.pcapng`";
    actual = client.queryBuilder().sql(queryWithDiffOrder).rowSet();

    expectedSchema = new TupleSchema();

    expectedSchema.add(MaterializedField.create("timestamp", Types.required(TypeProtos.MinorType.TIMESTAMP)));
    expectedSchema.add(MaterializedField.create("src_ip", Types.optional(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(MaterializedField.create("dst_ip", Types.optional(TypeProtos.MinorType.VARCHAR)));
    expectedSchema.add(MaterializedField.create("src_port", Types.optional(TypeProtos.MinorType.INT)));
    expectedSchema.add(MaterializedField.create("tcp_session", Types.optional(TypeProtos.MinorType.BIGINT)));
    expectedSchema.add(MaterializedField.create("dst_mac_address", Types.optional(TypeProtos.MinorType.VARCHAR)));

    expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(actual);
  }

  @Test
  public void testValidHeaderForArrayColumns() throws IOException {
    // query with non-existent field
    String query = "select arr[3] as arr from dfs.`store/pcapng/sniff.pcapng`";
    RowSet actual = client.queryBuilder().sql(query).rowSet();

    TupleSchema expectedSchema = new TupleSchema();

    expectedSchema.add(MaterializedField.create("arr", Types.optional(TypeProtos.MinorType.INT)));

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(actual);

    // query with an existent field which doesn't support arrays
    query = "select type[45] as arr from dfs.`store/pcapng/sniff.pcapng`";

    expectedSchema = new TupleSchema();
    actual = client.queryBuilder().sql(query).rowSet();

    expectedSchema.add(MaterializedField.create("arr", Types.optional(TypeProtos.MinorType.INT)));

    expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(actual);
  }

  @Test
  public void testValidHeaderForNestedColumns() throws IOException {
    // query with non-existent field
    String query = "select top['nested'] as nested from dfs.`store/pcapng/sniff.pcapng`";
    RowSet actual = client.queryBuilder().sql(query).rowSet();

    TupleSchema expectedSchema = new TupleSchema();

    expectedSchema.add(MaterializedField.create("nested", Types.optional(TypeProtos.MinorType.INT)));

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(actual);

    // query with an existent field which doesn't support nesting
    query = "select type['nested'] as nested from dfs.`store/pcapng/sniff.pcapng`";

    expectedSchema = new TupleSchema();
    actual = client.queryBuilder().sql(query).rowSet();

    expectedSchema.add(MaterializedField.create("nested", Types.optional(TypeProtos.MinorType.INT)));

    expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .build();
    new RowSetComparison(expected)
        .verifyAndClearAll(actual);
  }
}
