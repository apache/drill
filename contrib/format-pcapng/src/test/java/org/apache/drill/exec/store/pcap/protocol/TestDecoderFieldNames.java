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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Every decoder field must be usable in a query without backticks. A field named
 * after a reserved word such as from or timestamp is a parse error unless quoted.
 */
public class TestDecoderFieldNames extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("pcap/"));
  }

  private static void collect(TupleMetadata tuple, String path, List<String> out) {
    for (ColumnMetadata column : tuple) {
      String member = path + "." + column.name() + (column.isArray() && column.isMap() ? "[0]" : "");
      if (column.isMap()) {
        collect(column.tupleSchema(), member, out);
      } else {
        out.add(member);
      }
    }
  }

  private boolean parses(String field) {
    try {
      client.queryBuilder().sql("select " + field + " from dfs.`pcap/http.pcap` t").explainText();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Test
  public void testReservedWordIsDetected() {
    // Guards the check below: a reserved word must fail to parse
    assertTrue(!parses("t.parsed_data.x.from"));
  }

  @Test
  public void testFieldsNeedNoQuoting() {
    List<String> fields = new ArrayList<>();
    collect(ProtocolDecoders.get().packetDataSchema(), "t.parsed_data", fields);
    collect(ProtocolDecoders.get().sessionDataSchema(), "t.parsed_data", fields);
    List<String> failed = new ArrayList<>();
    for (String field : fields) {
      if (!parses(field)) {
        failed.add(field);
      }
    }
    if (!failed.isEmpty()) {
      fail("Decoder fields that are reserved words: " + failed);
    }
  }
}
