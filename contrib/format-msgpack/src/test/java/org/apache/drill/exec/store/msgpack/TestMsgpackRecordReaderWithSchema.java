package org.apache.drill.exec.store.msgpack;
/*
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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.msgpack.MsgpackFormatPlugin.MsgpackFormatConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryRowSetIterator;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;

public class TestMsgpackRecordReaderWithSchema extends ClusterTest {
  private static File testDir;
  private static File schemaLocation;

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher).maxParallelization(1));

    MsgpackFormatConfig msgFormat = new MsgpackFormatConfig();
    msgFormat.setSkipMalformedMsgRecords(true);
    msgFormat.setPrintSkippedMalformedMsgRecordLineNumber(true);
    msgFormat.setLearnSchema(true);
    msgFormat.setUseSchema(true);
    msgFormat.setExtensions(ImmutableList.of("mp"));
    testDir = cluster.makeDataDir("withmeta", "msgpack", msgFormat);
    schemaLocation = new File(testDir, ".schema.proto");

    if (TestMsgpackRecordReaderWithSchema.schemaLocation.exists()) {
      boolean deleted = TestMsgpackRecordReaderWithSchema.schemaLocation.delete();
      assertTrue(deleted);
    }
    completeModel();
    secondBatchHasCompleteModel();
    learnModel();
  }

  @Before
  public void before() throws Exception {
  }

  @Test
  public void testBasic() throws Exception {

    String sql = "select root.mapOfmap.aMap.sho as w from dfs.withmeta.`secondBatchHasCompleteModel.mp` as root";
    QueryRowSetIterator it = client.queryBuilder().sql(sql).rowSetIterator();
    assertTrue(it.hasNext());
    DirectRowSet batch1 = it.next();
    assertTrue(it.hasNext());
    DirectRowSet batch2 = it.next();
    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("w", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();
    //@formatter:on

    RowSetBuilder b = new RowSetBuilder(client.allocator(), expectedSchema);
    for (int i = 0; i < MsgpackRecordReader.DEFAULT_ROWS_PER_BATCH; i++) {
      b.addRow(new Object[] { null });
    }
    RowSet expected = b.build();
    new RowSetComparison(expected).verifyAndClearAll(batch1);

    b = new RowSetBuilder(client.allocator(), expectedSchema);
    b.addRow(222L);
    expected = b.build();
    new RowSetComparison(expected).verifyAndClearAll(batch2);
  }

  private static void learnModel() throws RpcException {
    String sql = "select * from `dfs.withmeta`.`completeModel.mp`";
    List<QueryDataBatch> results = client.queryBuilder().sql(sql).results();
    for (QueryDataBatch batch : results) {
      batch.release();
    }
  }

  private static MessagePacker makeMessagePacker(String fileName) throws IOException {
    return MessagePack.newDefaultPacker(new FileOutputStream(new File(testDir, fileName)));
  }

  public static void secondBatchHasCompleteModel() throws IOException {
    MessagePacker packer = makeMessagePacker("secondBatchHasCompleteModel.mp");

    for (int i = 0; i < MsgpackRecordReader.DEFAULT_ROWS_PER_BATCH; i++) {
      packer.packMapHeader(1);
      packer.packString("int").packInt(0);
    }

    writeCompleteModel(packer);
    packer.close();
  }

  public static void completeModel() throws IOException {
    MessagePacker packer = makeMessagePacker("completeModel.mp");

    writeCompleteModel(packer);

    packer.close();
  }

  private static void writeCompleteModel(MessagePacker packer) throws IOException {
    packer.packMapHeader(16);

    // 0
    packer.packString("arrayOfArray");
    packer.packArrayHeader(2);
    packer.packArrayHeader(3);
    packer.packInt(1);
    packer.packInt(1);
    packer.packInt(1);
    packer.packArrayHeader(5);
    packer.packInt(1);
    packer.packInt(1);
    packer.packInt(1);
    packer.packInt(1);
    packer.packInt(1);
    // 1
    packer.packString("arrayOfMap");
    packer.packArrayHeader(2);
    packer.packMapHeader(10);
    // 10 sub fields
    addAllFieldTypes(packer);
    packer.packMapHeader(10);
    // 10 sub fields
    addAllFieldTypes(packer);

    // 2
    packer.packString("mapOfMap");
    packer.packMapHeader(1).packString("aMap").packMapHeader(10);
    // 10 sub fields
    addAllFieldTypes(packer);

    // 3
    packer.packString("mapWithArray");
    packer.packMapHeader(2);
    packer.packString("anArray").packArrayHeader(2).packString("v1").packString("v2");
    packer.packString("aString").packString("x");

    // 4
    packer.packString("arrayWithMap");
    packer.packArrayHeader(2);
    packer.packMapHeader(1).packString("x").packInt(1);
    packer.packMapHeader(1).packString("y").packFloat(2.2f);

    // 10 + 4
    addAllFieldTypes(packer);
    // 15
    packer.packString("arrayOfInt");
    packer.packArrayHeader(2);
    packer.packInt(1);
    packer.packInt(1);
  }

  private static void addAllFieldTypes(MessagePacker packer) throws IOException {
    byte[] bytes = "some data".getBytes();
    packer.packString("raw").packRawStringHeader(bytes.length).addPayload(bytes);

    byte[] binary = { 0x1, 0x2, 0x9, 0xF };
    packer.packString("bin").packBinaryHeader(binary.length).addPayload(binary);

    packer.packString("int").packInt(32_000_000);
    packer.packString("big").packBigInteger(BigInteger.valueOf(64_000_000_000L));
    packer.packString("byt").packByte((byte) 129);
    packer.packString("lon").packLong(64_000_000_000L);
    packer.packString("nil").packNil();
    packer.packString("sho").packShort((short) 222);
    packer.packString("dou").packDouble(1.1d);
    packer.packString("flo").packFloat(1.1f);
  }

}
