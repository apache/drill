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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.ZoneId;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.msgpack.MsgpackFormatPlugin.MsgpackFormatConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;

public class TestMsgpackRecordReader extends ClusterTest {
  private static File testDir;

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher).maxParallelization(1));

    MsgpackFormatConfig msgFormat = new MsgpackFormatConfig();
    msgFormat.setSkipMalformedMsgRecords(true);
    msgFormat.setPrintSkippedMalformedMsgRecordLineNumber(true);
    msgFormat.setExtensions(ImmutableList.of("mp"));
    testDir = cluster.makeDataDir("data", "msgpack", msgFormat);
  }

  @Test
  public void testBasic() throws Exception {
    String fileName = "basic.mp";
    buildBasic(fileName);

    String sql = "select * from `dfs.data`.`" + fileName + "`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("apple", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("banana", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("orange", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("potato", TypeProtos.MinorType.FLOAT8, TypeProtos.DataMode.OPTIONAL).buildSchema();

    //@formatter:off
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1L, 2L, "infini!!!", null)
        .addRow(1L, 2L, null, 12.12d)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testBasicSelect() throws Exception {
    String fileName = "basicSelect.mp";
    buildBasic(fileName);

    String sql = "select `apple` as y from `dfs.data`.`" + fileName + "`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("y", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1L)
        .addRow(1L)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testSelectMap() throws Exception {
    String fileName = "selectMap.mp";
    buildNested(fileName);

    String sql = "select root.aMap.b as x from `dfs.data`.`" + fileName + "` as root";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(44L)
        .addRow(new Object[] {null})
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testSelectArray() throws Exception {
    String fileName = "selectArray.mp";
    buildNested(fileName);

    String sql = "select round(root.anArray[1], 3) as x from `dfs.data`.`" + fileName + "` as root";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.FLOAT8, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(new Object[] {null})
        .addRow(0.342d)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testTypes() throws Exception {
    String fileName = "types.mp";
    buildTypes(fileName);

    String sql = "select * from `dfs.data`.`" + fileName + "` as root";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    // nil column will not be read at all.

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("raw", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .add("bin", TypeProtos.MinorType.VARBINARY, TypeProtos.DataMode.OPTIONAL)
        .add("int", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("big", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("byt", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("lon", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("sho", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("dou", TypeProtos.MinorType.FLOAT8, TypeProtos.DataMode.OPTIONAL)
        .add("flo", TypeProtos.MinorType.FLOAT8, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(
            "some data",
            new byte[]{0x1, 0x2, 0x9, 0xF},
            32_000_000L,
            64_000_000_000L,
            77L,
            64_000_000_000L,
            222L,
            1.1d,
            1.1f)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testSelectMapOfMap() throws Exception {
    String fileName = "selectMapOfMap.mp";
    buildNestedMapOfMap(fileName);

    String sql = "select root.mapOfMap.aMap.x as x from `dfs.data`.`" + fileName + "` as root";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1L)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testSelectArrayOfMap() throws Exception {
    String fileName = "selectArrayOfMap.mp";
    buildNestedArrayOfMap(fileName);

    String sql = "select root.arrayOfMap[0].x as x from `dfs.data`.`" + fileName + "` as root";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1L)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testSelectArrayOfMap2() throws Exception {
    String fileName = "selectArrayOfMap.mp";
    buildNestedArrayOfMap(fileName);

    String sql = "select root.arrayOfMap[1].x as x from `dfs.data`.`" + fileName + "` as root";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1L)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testSelectMapWithArray() throws Exception {
    String fileName = "selectMapWithArray.mp";
    buildNestedMapWithArray(fileName);

    String sql = "select root.mapWithArray.anArray[1] as x from `dfs.data`.`" + fileName + "` as root";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow("v2")
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testdMissingFieldName() throws Exception {
    String fileName = "missingFieldName.mp";
    buildMissingFieldName(fileName);

    String sql = "select * from `dfs.data`.`" + fileName + "`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("y", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("z", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1L, 2L, 3L)
        // Incomplete MAP are skipped
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testdCountInvalidMessages() throws Exception {
    String fileName = "invalidMessages.mp";
    buildInvalidMessages(fileName);

    String sql = "select count(1) as c from `dfs.data`.`" + fileName + "`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("c", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.REQUIRED)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1002L)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testTimestamp() throws Exception {
    LocalDate now = LocalDate.now(ZoneId.of("UTC"));
    long epochSeconds = now.atStartOfDay(ZoneId.of("UTC")).toEpochSecond();
    long nanoSeconds = 0;

    String fileName = "timestamp.mp";
    buildTimestamp(fileName, epochSeconds, nanoSeconds);

    String sql = "select * from `dfs.data`.`" + fileName + "`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("timestamp", TypeProtos.MinorType.TIMESTAMP, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(epochSeconds)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testTimestampWithNanos() throws Exception {
    LocalDate now = LocalDate.now(ZoneId.of("UTC"));
    long epochSeconds = now.atStartOfDay(ZoneId.of("UTC")).toEpochSecond();
    long nanoSeconds = 900_000;

    String fileName = "timestamp.mp";
    buildTimestamp(fileName, epochSeconds, nanoSeconds);

    String sql = "select * from `dfs.data`.`" + fileName + "`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("timestamp", TypeProtos.MinorType.TIMESTAMP, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    // we are ignoring the nanos
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(epochSeconds)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testTimestampLargeWithNanos() throws Exception {
    long epochSeconds = (long)Math.pow(2, 35);
    long nanoSeconds = 900_000;

    String fileName = "timestamp.mp";
    buildTimestamp(fileName, epochSeconds, nanoSeconds);

    String sql = "select * from `dfs.data`.`" + fileName + "`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("timestamp", TypeProtos.MinorType.TIMESTAMP, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    // we are ignoring the nanos
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(epochSeconds)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testUnknownExt() throws Exception {
    String fileName = "unknownExt.mp";
    buildUnknownExt(fileName);

    String sql = "select * from `dfs.data`.`" + fileName + "`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.VARBINARY, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(new byte[] {'j','k'})
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testMapWithNonStringFieldName() throws Exception {
    String fileName = "testMapWithNonStringFieldName.mp";
    buildMapWithNonStringFieldName(fileName);

    String sql = "select * from `dfs.data`.`" + fileName + "`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("1", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("A", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("3", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("xyz", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .add("5", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();

    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
        .addRow(1L, 2L, 3L, 4L, 5L)
        .build();
    //@formatter:on
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  @Test
  public void testIntSizeChanging() throws Exception {
    String fileName = "intSizeChanging.mp";
    buildIntColumn(fileName);

    String sql = "select x from `dfs.data`.`" + fileName + "`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.BIGINT, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();
    //@formatter:on
    RowSetBuilder b = new RowSetBuilder(client.allocator(), expectedSchema);
    for (long i = 0; i < 1000; i++) {
      b.addRow(i);
    }

    b.addRow(Long.MAX_VALUE);
    b.addRow(Long.MAX_VALUE);
    RowSet expected = b.build();
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }
  
  @Test
  public void testVarcharNotPresentInFirstBatches() throws Exception {
    String fileName = "varcharNotPresentInFirstBatches.mp";
    buildVarcharColumn(fileName);

    String sql = "select x from `dfs.data`.`" + fileName + "`";
    RowSet actual = client.queryBuilder().sql(sql).rowSet();

    //@formatter:off
    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("x", TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL)
        .buildSchema();
    //@formatter:on
    RowSetBuilder b = new RowSetBuilder(client.allocator(), expectedSchema);
    for (long i = 0; i < 10000; i++) {
      b.addRow(new Object[] {null});
    }
    b.addRow(1);
    b.addRow(2);

    RowSet expected = b.build();
    new RowSetComparison(expected).verifyAndClearAll(actual);
  }

  private MessagePacker makeMessagePacker(String fileName) throws IOException {
    return MessagePack.newDefaultPacker(new FileOutputStream(new File(testDir, fileName)));
  }

  private void buildBasic(String fileName) throws IOException {
    MessagePacker packer = makeMessagePacker(fileName);

    packer.packMapHeader(3);
    packer.packString("apple").packInt(1);
    packer.packString("banana").packInt(2);
    packer.packString("orange").packString("infini!!!");

    packer.packMapHeader(3);
    packer.packString("apple").packInt(1);
    packer.packString("banana").packInt(2);
    packer.packString("potato").packDouble(12.12);

    packer.close();
  }

  private void buildNested(String fileName) throws IOException {
    MessagePacker packer = makeMessagePacker(fileName);

    packer.packMapHeader(1);
    packer.packString("aMap");
    packer.packMapHeader(2);
    packer.packString("a").packInt(33);
    packer.packString("b").packInt(44);

    packer.packMapHeader(1);
    packer.packString("anArray");
    packer.packArrayHeader(2).packFloat(0.1f).packFloat(0.342f);

    packer.close();
  }

  private void buildNestedArrayOfMap(String fileName) throws IOException {
    MessagePacker packer = makeMessagePacker(fileName);

    packer.packMapHeader(1);
    packer.packString("arrayOfMap");
    packer.packArrayHeader(2);
    packer.packMapHeader(1).packString("x").packInt(1);
    packer.packMapHeader(1).packString("x").packInt(1);

    packer.close();
  }

  private void buildNestedMapOfMap(String fileName) throws IOException {
    MessagePacker packer = makeMessagePacker(fileName);

    packer.packMapHeader(1);
    packer.packString("mapOfMap");
    packer.packMapHeader(2);
    packer.packString("aMap").packMapHeader(1).packString("x").packInt(1);
    packer.packString("aString").packString("x");

    packer.close();
  }

  private void buildNestedMapWithArray(String fileName) throws IOException {
    MessagePacker packer = makeMessagePacker(fileName);

    packer.packMapHeader(1);
    packer.packString("mapWithArray");
    packer.packMapHeader(2);
    packer.packString("anArray").packArrayHeader(2).packString("v1").packString("v2");
    packer.packString("aString").packString("x");

    packer.close();
  }

  private void buildTypes(String fileName) throws IOException {
    MessagePacker packer = makeMessagePacker(fileName);

    byte[] bytes = "some data".getBytes();
    byte[] binary = { 0x1, 0x2, 0x9, 0xF };
    packer.packMapHeader(10);
    packer.packString("raw").packRawStringHeader(bytes.length).addPayload(bytes);
    packer.packString("bin").packBinaryHeader(binary.length).addPayload(binary);
    packer.packString("int").packInt(32_000_000);
    packer.packString("big").packBigInteger(BigInteger.valueOf(64_000_000_000L));
    packer.packString("byt").packByte((byte) 77);
    packer.packString("lon").packLong(64_000_000_000L);
    packer.packString("nil").packNil();
    packer.packString("sho").packShort((short) 222);
    packer.packString("dou").packDouble(1.1d);
    packer.packString("flo").packFloat(1.1f);

    packer.close();
  }

  private void buildIntColumn(String fileName) throws IOException {
    MessagePacker packer = makeMessagePacker(fileName);

    for (long i = 0; i < 1000; i++) {
      packer.packMapHeader(1);
      packer.packString("x").packLong(i);
    }

    packer.packMapHeader(1);
    packer.packString("x").packLong(Long.MAX_VALUE);
    packer.packMapHeader(1);
    packer.packString("x").packLong(Long.MAX_VALUE);

    packer.close();
  }

  private void buildVarcharColumn(String fileName) throws IOException {
    MessagePacker packer = makeMessagePacker(fileName);

    // messages with no x column
    for (long i = 0; i < 10000; i++) {
      packer.packMapHeader(1);
      packer.packString("y").packLong(i);
    }

    packer.packMapHeader(2);
    packer.packString("x").packString("data");
    packer.packString("y").packLong(1);
    packer.packMapHeader(2);
    packer.packString("x").packString("data2");
    packer.packString("y").packLong(2);

    packer.close();
  }

  private void buildMissingFieldName(String fileName) throws IOException {
    MessagePacker packer = makeMessagePacker(fileName);

    packer.packMapHeader(3);
    packer.packString("x").packLong(1);
    packer.packString("y").packLong(2);
    packer.packString("z").packLong(3);

    packer.packMapHeader(3);
    packer.packString("x").packLong(1);
    packer./* missing */packLong(2);
    packer.packString("z").packLong(3);
    packer.close();
  }

  private void buildInvalidMessages(String fileName) throws IOException {
    MessagePacker packer = makeMessagePacker(fileName);

    for (int i = 0; i < 1000; i++) {
      packer.packMapHeader(1);
      packer.packString("x").packLong(1);
    }
    // missing field name, all these maps are skipped
    for (int i = 0; i < 100; i++) {
      packer.packMapHeader(1);
      packer.packLong(1);
    }

    // one map with invalid fields
    packer.packMapHeader(3);
    // 1. good tuple
    packer.packString("x").packLong(1);
    // 2. map is not a good field name, skipped
    packer.packMapHeader(1);
    packer.packString("x").packLong(1);
    // 2. map is not a good field name, skipped
    packer.packMapHeader(1);
    packer.packString("x").packLong(1);
    // 2. tuple okay
    packer.packString("x").packLong(1);
    // 3. tuple okay
    packer.packString("x").packLong(1);

    // out of bad map

    // one more good map
    packer.packMapHeader(1);
    packer.packString("x").packLong(1);

    packer.close();
  }

  private void buildMapWithNonStringFieldName(String fileName) throws IOException {
    MessagePacker packer = makeMessagePacker(fileName);

    packer.packMapHeader(5);
    // Int
    packer.packInt(1).packLong(1);
    // Binary
    packer.packBinaryHeader(1).addPayload(new byte[] { 'A' }).packLong(2);
    // Long
    packer.packLong(3).packLong(3);
    // Raw string
    String name = "xyz";
    packer.packRawStringHeader(name.length()).addPayload(name.getBytes()).packLong(4);
    // Short
    packer.packShort((short) 5).packLong(5);
    packer.close();
  }

  private void buildTimestamp(String fileName, long epochSeconds, long nanoSeconds) throws IOException {
    MessagePacker packer = makeMessagePacker(fileName);

    byte TIMESTAMP_TYPE = (byte) -1;

    packer.packMapHeader(1);
    packer.packString("timestamp");
    byte[] bytes = getBytes(epochSeconds, nanoSeconds);
    packer.packExtensionTypeHeader(TIMESTAMP_TYPE, bytes.length);
    packer.addPayload(bytes);
    packer.close();
  }

  private byte[] getBytes(long epochSeconds, long nanoSeconds) {
    if (epochSeconds >> 34 == 0) {
      long data64 = (nanoSeconds << 34) | epochSeconds;
      if ((data64 & 0xffffffff00000000L) == 0) {
        // timestamp 32
        return getBytes(data64, 4);
      } else {
        // timestamp 64
        return getBytes(data64, 8);
      }
    } else {
      byte[] nanoUnsigned = getBytes(nanoSeconds, 4);
      byte[] epochSecondsSigned = getBytes(epochSeconds, 8);
      byte[] ret = new byte[12];
      System.arraycopy(nanoUnsigned, 0, ret, 0, 4);
      System.arraycopy(epochSecondsSigned, 0, ret, 4, 8);
      return ret;
    }
  }

  private byte[] getBytes(long data64, int n) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(data64);
    byte[] bytes = new byte[n];
    buffer.position(Long.BYTES - n);
    buffer.get(bytes);
    return bytes;
  }

  private void buildUnknownExt(String fileName) throws IOException {
    MessagePacker packer = makeMessagePacker(fileName);

    byte type = (byte) 7; // unknown msgpack extension code

    byte[] bytes = new byte[2];
    bytes[0] = 'j';
    bytes[1] = 'k';

    packer.packMapHeader(1);
    packer.packString("x").packExtensionTypeHeader(type, bytes.length);
    packer.addPayload(bytes);
    packer.close();
  }
}
