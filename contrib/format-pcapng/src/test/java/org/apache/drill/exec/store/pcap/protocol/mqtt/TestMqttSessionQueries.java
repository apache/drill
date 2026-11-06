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
package org.apache.drill.exec.store.pcap.protocol.mqtt;

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

/** Query tests of the MQTT session decoder on a generated capture. */
public class TestMqttSessionQueries extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("decoders/"));
  }

  private static String sessions(String options) {
    return "table(dfs.`decoders/mqtt/mqtt.pcapng` (type => 'pcapng', sessionizeTCPStreams => true"
        + options + ")) t";
  }

  @Test
  public void testMqtt() throws Exception {
    String sql = "select src_port, parsed_protocol, t.parsed_data.mqtt.protocol_level as protocol_level, "
        + "t.parsed_data.mqtt.client_id as client_id, t.parsed_data.mqtt.user_name as user_name, "
        + "t.parsed_data.mqtt.password_present as password_present, t.parsed_data.mqtt.password as password, "
        + "t.parsed_data.mqtt.will_topic as will_topic, t.parsed_data.mqtt.connect_return_code as code, "
        + "t.parsed_data.mqtt.connect_result as result, t.parsed_data.mqtt.subscribed_topics[0] as sub0, "
        + "t.parsed_data.mqtt.published_topics[0] as pub0, t.parsed_data.mqtt.packet_count as packet_count, "
        + "decode_error from " + sessions("") + " order by src_port";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_port", MinorType.INT)
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("protocol_level", MinorType.INT)
        .addNullable("client_id", MinorType.VARCHAR)
        .addNullable("user_name", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addNullable("password", MinorType.VARCHAR)
        .addNullable("will_topic", MinorType.VARCHAR)
        .addNullable("code", MinorType.INT)
        .addNullable("result", MinorType.VARCHAR)
        .addNullable("sub0", MinorType.VARCHAR)
        .addNullable("pub0", MinorType.VARCHAR)
        .addNullable("packet_count", MinorType.INT)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow(52001, "mqtt", 4, "sensor-1", "mqttuser", true, null, "device/status", 0, "Connection Accepted",
            "home/#", "home/temp", 4, null)
        .addRow(52002, null, null, null, null, null, null, null, null, null, null, null, null, null)
        .addRow(52003, "mqtt", null, null, null, null, null, null, null, null, null, null, null,
            "mqtt: truncated CONNECT client identifier")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testMqttExposedPassword() throws Exception {
    String sql = "select t.parsed_data.mqtt.password as password from "
        + sessions(", exposeCredentials => true") + " where src_port = 52001";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("password", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("mqttpass")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
